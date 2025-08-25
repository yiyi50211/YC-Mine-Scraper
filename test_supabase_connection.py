#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Supabase连接测试脚本
测试数据库连接和表访问权限
"""

import os
import sys
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

def test_supabase_connection():
    """测试Supabase连接和表访问"""
    print("🔍 开始测试Supabase连接...")
    print("=" * 50)
    
    # 加载环境变量
    load_dotenv()
    
    # 获取Supabase配置
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_ANON_KEY')
    
    print(f"📋 配置检查:")
    print(f"   SUPABASE_URL: {'✅ 已设置' if supabase_url else '❌ 未设置'}")
    print(f"   SUPABASE_ANON_KEY: {'✅ 已设置' if supabase_key else '❌ 未设置'}")
    
    if not supabase_url or not supabase_key:
        print("\n❌ 环境变量配置不完整，请检查.env文件")
        return False
    
    try:
        # 创建Supabase客户端
        supabase: Client = create_client(supabase_url, supabase_key)
        print(f"\n✅ Supabase客户端创建成功")
        
        # 测试的表名列表
        tables_to_test = [
            'hiring_companies',
            'yc_jobs', 
            'yc_jobs_join'
        ]
        
        print(f"\n🗄️ 测试表访问权限:")
        print("-" * 30)
        
        table_results = {}
        
        for table_name in tables_to_test:
            try:
                print(f"\n📊 测试表: {table_name}")
                
                # 测试查询权限 - 获取表结构信息
                result = supabase.table(table_name).select("*").limit(1).execute()
                
                if result.data is not None:
                    print(f"   ✅ 查询权限: 正常")
                    
                    # 获取准确的记录总数
                    try:
                        count_result = supabase.table(table_name).select("*", count="exact").execute()
                        record_count = count_result.count if hasattr(count_result, 'count') else 0
                        print(f"   📈 当前记录数: {record_count}")
                    except Exception as count_error:
                        print(f"   ⚠️ 无法获取准确记录数: {str(count_error)}")
                        record_count = len(result.data)
                        print(f"   📈 样本记录数: {record_count}")
                    
                    # 如果有数据，显示字段信息
                    if record_count > 0:
                        fields = list(result.data[0].keys())
                        print(f"   🏷️ 字段数量: {len(fields)}")
                        print(f"   📝 字段列表: {', '.join(fields[:10])}{'...' if len(fields) > 10 else ''}")
                    else:
                        print(f"   📝 表为空，无法获取字段信息")
                    
                    table_results[table_name] = {
                        'accessible': True,
                        'record_count': record_count,
                        'fields': list(result.data[0].keys()) if record_count > 0 else []
                    }
                else:
                    print(f"   ⚠️ 查询返回None")
                    table_results[table_name] = {
                        'accessible': False,
                        'error': 'Query returned None'
                    }
                    
            except Exception as e:
                print(f"   ❌ 访问失败: {str(e)}")
                table_results[table_name] = {
                    'accessible': False,
                    'error': str(e)
                }
        
        # 测试插入权限（使用测试数据）
        print(f"\n🔧 测试写入权限:")
        print("-" * 20)
        
        test_table = 'hiring_companies'  # 使用hiring_companies表进行写入测试
        try:
            # 创建测试数据
            test_data = {
                'name': f'TEST_COMPANY_{datetime.now().strftime("%Y%m%d_%H%M%S")}',
                'slug': 'test-company',
                'website': 'https://test.com',
                'created_at': datetime.now().isoformat(),
                'updated_at': datetime.now().isoformat()
            }
            
            print(f"📝 尝试插入测试数据到 {test_table} 表...")
            
            # 插入测试数据
            insert_result = supabase.table(test_table).insert(test_data).execute()
            
            if insert_result.data:
                inserted_id = insert_result.data[0].get('id')
                print(f"   ✅ 插入成功，ID: {inserted_id}")
                
                # 立即删除测试数据
                delete_result = supabase.table(test_table).delete().eq('id', inserted_id).execute()
                if delete_result:
                    print(f"   🗑️ 测试数据已清理")
                else:
                    print(f"   ⚠️ 测试数据清理可能失败")
                    
                print(f"   ✅ 写入权限: 正常")
            else:
                print(f"   ❌ 插入失败: 无返回数据")
                
        except Exception as e:
            print(f"   ❌ 写入测试失败: {str(e)}")
        
        # 汇总报告
        print(f"\n📊 测试结果汇总:")
        print("=" * 50)
        
        accessible_tables = [name for name, info in table_results.items() if info.get('accessible', False)]
        failed_tables = [name for name, info in table_results.items() if not info.get('accessible', False)]
        
        print(f"✅ 可访问的表 ({len(accessible_tables)}/{len(tables_to_test)}):")
        for table_name in accessible_tables:
            info = table_results[table_name]
            print(f"   - {table_name}: {info['record_count']}条记录, {len(info['fields'])}个字段")
        
        if failed_tables:
            print(f"\n❌ 无法访问的表 ({len(failed_tables)}/{len(tables_to_test)}):")
            for table_name in failed_tables:
                error = table_results[table_name].get('error', '未知错误')
                print(f"   - {table_name}: {error}")
        
        # 总体状态
        if len(accessible_tables) == len(tables_to_test):
            print(f"\n🎉 所有表访问正常！可以进行数据更新操作。")
            return True
        else:
            print(f"\n⚠️ 部分表访问异常，请检查数据库配置和权限。")
            return False
            
    except Exception as e:
        print(f"\n❌ Supabase连接失败: {str(e)}")
        return False

def main():
    """主函数"""
    print("🚀 Supabase连接测试工具")
    print(f"⏰ 测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    success = test_supabase_connection()
    
    print(f"\n{'='*50}")
    if success:
        print("✅ 测试完成：Supabase连接和表访问正常")
        print("💡 提示：现在可以运行 update_supabase_tables.py 进行数据更新")
    else:
        print("❌ 测试完成：发现问题，请检查配置")
        print("💡 提示：请检查.env文件中的Supabase配置和数据库表权限")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())