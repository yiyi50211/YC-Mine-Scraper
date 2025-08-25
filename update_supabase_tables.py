#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
自动更新Supabase数据库脚本
从result文件夹中查找最新的CSV文件并更新到对应的数据库表中
使用supabase-py客户端进行连接
"""

import os
import sys
import glob
import pandas as pd
import json
from datetime import datetime
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

class SupabaseUpdater:
    """
    Supabase数据库更新器
    """
    
    def __init__(self):
        # 加载环境变量
        load_dotenv()
        
        # 获取Supabase配置
        self.supabase_url = os.getenv('SUPABASE_URL')
        self.supabase_key = os.getenv('SUPABASE_ANON_KEY')
        self.supabase: Client = None
        
        # 设置日志
        os.makedirs('logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'logs/supabase_update_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def connect_to_database(self):
        """
        连接到Supabase数据库
        """
        try:
            if not self.supabase_url or not self.supabase_key:
                raise ValueError("Supabase配置不完整，请检查.env文件")
            
            self.logger.info("正在连接到Supabase数据库...")
            self.supabase = create_client(self.supabase_url, self.supabase_key)
            
            # 测试连接
            test_result = self.supabase.table('hiring_companies').select("*").limit(1).execute()
            if test_result.data is not None:
                self.logger.info("✅ Supabase数据库连接成功")
                return True
            else:
                raise Exception("连接测试失败")
                
        except Exception as e:
            self.logger.error(f"❌ 数据库连接失败: {str(e)}")
            return False
    
    def find_latest_file(self, pattern):
        """
        查找最新的文件
        """
        try:
            # 首先在result目录中查找
            result_files = glob.glob(f"result/{pattern}")
            # 然后在根目录中查找（向后兼容）
            root_files = glob.glob(pattern)
            
            all_files = result_files + root_files
            
            if not all_files:
                self.logger.warning(f"未找到匹配模式 {pattern} 的文件")
                return None
            
            # 按修改时间排序，返回最新的文件
            latest_file = max(all_files, key=os.path.getmtime)
            self.logger.info(f"找到最新文件: {latest_file}")
            return latest_file
            
        except Exception as e:
            self.logger.error(f"查找文件时出错: {str(e)}")
            return None
    
    def get_table_record_count(self, table_name):
        """
        获取表的记录数量
        """
        try:
            result = self.supabase.table(table_name).select("*", count="exact").execute()
            return result.count if hasattr(result, 'count') else 0
        except Exception as e:
            self.logger.warning(f"无法获取表 {table_name} 的记录数: {str(e)}")
            return 0
    
    def clear_table(self, table_name):
        """
        清空表数据
        """
        try:
            self.logger.info(f"正在清空表 {table_name}...")
            
            # 获取当前记录数
            current_count = self.get_table_record_count(table_name)
            self.logger.info(f"表 {table_name} 当前有 {current_count} 条记录")
            
            if current_count == 0:
                self.logger.info(f"表 {table_name} 已经为空，跳过清空操作")
                return True
            
            # 删除所有记录 - 使用neq来删除所有记录
            result = self.supabase.table(table_name).delete().neq('id', 0).execute()
            
            # 验证删除结果
            new_count = self.get_table_record_count(table_name)
            self.logger.info(f"✅ 表 {table_name} 清空完成，剩余记录: {new_count}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ 清空表 {table_name} 失败: {str(e)}")
            return False
    
    def batch_insert_data(self, table_name, data, batch_size=100):
        """
        批量插入数据
        """
        try:
            total_records = len(data)
            self.logger.info(f"开始批量插入 {total_records} 条记录到表 {table_name}")
            
            success_count = 0
            error_count = 0
            
            # 分批插入
            for i in range(0, total_records, batch_size):
                batch_data = data[i:i + batch_size]
                batch_num = i // batch_size + 1
                total_batches = (total_records + batch_size - 1) // batch_size
                
                try:
                    self.logger.info(f"插入第 {batch_num}/{total_batches} 批数据 ({len(batch_data)} 条记录)")
                    
                    result = self.supabase.table(table_name).insert(batch_data).execute()
                    
                    if result.data:
                        success_count += len(batch_data)
                        self.logger.info(f"✅ 第 {batch_num} 批插入成功")
                    else:
                        error_count += len(batch_data)
                        self.logger.error(f"❌ 第 {batch_num} 批插入失败: 无返回数据")
                        
                except Exception as batch_error:
                    error_count += len(batch_data)
                    self.logger.error(f"❌ 第 {batch_num} 批插入失败: {str(batch_error)}")
            
            self.logger.info(f"批量插入完成: 成功 {success_count} 条，失败 {error_count} 条")
            return success_count, error_count
            
        except Exception as e:
            self.logger.error(f"❌ 批量插入数据失败: {str(e)}")
            return 0, len(data)
    
    def update_hiring_companies(self):
        """
        更新hiring_companies表
        """
        try:
            self.logger.info("=" * 50)
            self.logger.info("开始更新 hiring_companies 表")
            
            # 查找最新的公司数据文件
            latest_file = self.find_latest_file("all_hiring_companies_*.csv")
            if not latest_file:
                self.logger.error("未找到公司数据文件")
                return False
            
            # 读取CSV文件
            self.logger.info(f"正在读取文件: {latest_file}")
            df = pd.read_csv(latest_file)
            
            # 数据预处理
            df = df.fillna('')  # 填充空值
            
            # 转换为字典列表
            data = df.to_dict('records')
            self.logger.info(f"读取到 {len(data)} 条公司记录")
            
            # 清空现有数据
            if not self.clear_table('hiring_companies'):
                return False
            
            # 批量插入新数据
            success_count, error_count = self.batch_insert_data('hiring_companies', data)
            
            if error_count == 0:
                self.logger.info(f"✅ hiring_companies 表更新成功，共 {success_count} 条记录")
                return True
            else:
                self.logger.warning(f"⚠️ hiring_companies 表更新部分成功: {success_count} 成功，{error_count} 失败")
                return success_count > 0
                
        except Exception as e:
            self.logger.error(f"❌ 更新 hiring_companies 表失败: {str(e)}")
            return False
    
    def update_yc_jobs(self):
        """
        更新yc_jobs表
        """
        try:
            self.logger.info("=" * 50)
            self.logger.info("开始更新 yc_jobs 表")
            
            # 查找最新的职位数据文件
            latest_file = self.find_latest_file("all_jobs_*.csv")
            if not latest_file:
                self.logger.error("未找到职位数据文件")
                return False
            
            # 读取CSV文件
            self.logger.info(f"正在读取文件: {latest_file}")
            df = pd.read_csv(latest_file)
            
            # 数据预处理
            df = df.fillna('')  # 填充空值
            
            # 转换为字典列表
            data = df.to_dict('records')
            self.logger.info(f"读取到 {len(data)} 条职位记录")
            
            # 清空现有数据
            if not self.clear_table('yc_jobs'):
                return False
            
            # 批量插入新数据
            success_count, error_count = self.batch_insert_data('yc_jobs', data)
            
            if error_count == 0:
                self.logger.info(f"✅ yc_jobs 表更新成功，共 {success_count} 条记录")
                return True
            else:
                self.logger.warning(f"⚠️ yc_jobs 表更新部分成功: {success_count} 成功，{error_count} 失败")
                return success_count > 0
                
        except Exception as e:
            self.logger.error(f"❌ 更新 yc_jobs 表失败: {str(e)}")
            return False
    
    def update_yc_jobs_join(self):
        """
        更新yc_jobs_join表（合并后的数据）
        """
        try:
            self.logger.info("=" * 50)
            self.logger.info("开始更新 yc_jobs_join 表")
            
            # 查找最新的合并数据文件
            latest_file = self.find_latest_file("all_jobs_company_info_*.csv")
            if not latest_file:
                self.logger.error("未找到合并数据文件")
                return False
            
            # 读取CSV文件
            self.logger.info(f"正在读取文件: {latest_file}")
            df = pd.read_csv(latest_file)
            
            # 数据预处理
            df = df.fillna('')  # 填充空值
            
            # 转换为字典列表
            data = df.to_dict('records')
            self.logger.info(f"读取到 {len(data)} 条合并记录")
            
            # 清空现有数据
            if not self.clear_table('yc_jobs_join'):
                return False
            
            # 批量插入新数据
            success_count, error_count = self.batch_insert_data('yc_jobs_join', data)
            
            if error_count == 0:
                self.logger.info(f"✅ yc_jobs_join 表更新成功，共 {success_count} 条记录")
                return True
            else:
                self.logger.warning(f"⚠️ yc_jobs_join 表更新部分成功: {success_count} 成功，{error_count} 失败")
                return success_count > 0
                
        except Exception as e:
            self.logger.error(f"❌ 更新 yc_jobs_join 表失败: {str(e)}")
            return False
    
    def run_update(self):
        """
        执行完整的数据库更新流程
        """
        try:
            self.logger.info("🚀 开始Supabase数据库更新流程")
            self.logger.info(f"⏰ 更新时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            # 连接数据库
            if not self.connect_to_database():
                return False
            
            # 更新各个表
            results = {
                'hiring_companies': self.update_hiring_companies(),
                'yc_jobs': self.update_yc_jobs(),
                'yc_jobs_join': self.update_yc_jobs_join()
            }
            
            # 汇总结果
            self.logger.info("=" * 50)
            self.logger.info("📊 更新结果汇总:")
            
            success_count = 0
            for table_name, success in results.items():
                status = "✅ 成功" if success else "❌ 失败"
                self.logger.info(f"   {table_name}: {status}")
                if success:
                    success_count += 1
            
            if success_count == len(results):
                self.logger.info("🎉 所有表更新成功！")
                return True
            elif success_count > 0:
                self.logger.warning(f"⚠️ 部分表更新成功 ({success_count}/{len(results)})")
                return True
            else:
                self.logger.error("❌ 所有表更新失败")
                return False
                
        except Exception as e:
            self.logger.error(f"❌ 更新流程执行失败: {str(e)}")
            return False

def main():
    """
    主函数
    """
    print("🚀 Supabase数据库更新工具")
    print(f"⏰ 启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    updater = SupabaseUpdater()
    success = updater.run_update()
    
    print(f"\n{'='*50}")
    if success:
        print("✅ 数据库更新完成")
        print("💡 提示：所有表已成功更新到最新数据")
    else:
        print("❌ 数据库更新失败")
        print("💡 提示：请检查日志文件了解详细错误信息")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())