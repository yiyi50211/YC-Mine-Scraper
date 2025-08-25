#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整的数据爬取和合并脚本
按顺序执行：
1. 爬取最新的all_hiring_companies数据
2. 爬取最新的all_jobs数据  
3. 合并数据生成最终的all_jobs_company_info文件
"""

import subprocess
import time
import datetime
import os
import sys
import logging
from pathlib import Path

def setup_logging():
    """设置日志配置"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    log_file = os.path.join(log_dir, f"complete_scraper_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def run_script(script_name, logger):
    """
    运行指定的Python脚本并记录输出
    """
    try:
        logger.info(f"开始运行 {script_name}")
        start_time = time.time()
        
        # 检查脚本文件是否存在
        if not os.path.exists(script_name):
            logger.error(f"脚本文件不存在: {script_name}")
            return False
        
        # 运行脚本并捕获输出
        result = subprocess.run(
            ["python", script_name], 
            capture_output=True, 
            text=True, 
            check=True,
            encoding='utf-8',
            errors='ignore'
        )
        
        # 记录标准输出
        if result.stdout:
            for line in result.stdout.splitlines():
                logger.info(f"[{script_name}] {line}")
        
        # 记录标准错误（如果有）
        if result.stderr:
            for line in result.stderr.splitlines():
                logger.warning(f"[{script_name}] STDERR: {line}")
        
        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"✅ {script_name} 运行完成，耗时 {duration:.2f} 秒")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_name} 运行失败，错误代码: {e.returncode}")
        if e.stderr:
            logger.error(f"错误输出: {e.stderr}")
        if e.stdout:
            logger.error(f"标准输出: {e.stdout}")
        return False
    except Exception as e:
        logger.error(f"❌ {script_name} 运行时发生异常: {str(e)}")
        return False

def run_script_with_args(script_name, args, logger):
    """
    运行指定的Python脚本（带参数）并实时记录输出
    """
    try:
        logger.info(f"开始运行 {script_name} {' '.join(args)}")
        start_time = time.time()
        
        # 检查脚本文件是否存在
        if not os.path.exists(script_name):
            logger.error(f"脚本文件不存在: {script_name}")
            return False
        
        # 构建命令
        cmd = ["python", script_name] + args
        
        # 使用Popen实现实时输出
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='ignore',
            bufsize=1,
            universal_newlines=True
        )
        
        # 自动回答确认问题
        if process.stdin:
            process.stdin.write("y\n")
            process.stdin.flush()
            process.stdin.close()
        
        # 实时读取并记录输出
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                line = output.strip()
                if line:
                    logger.info(f"[{script_name}] {line}")
                    # 特别标记公司爬取进度
                    if "进度:" in line or "成功爬取公司" in line or "开始爬取公司" in line:
                        logger.info(f"🔍 {line}")
        
        # 等待进程完成
        return_code = process.wait()
        
        end_time = time.time()
        duration = end_time - start_time
        
        if return_code == 0:
            logger.info(f"✅ {script_name} 运行完成，耗时 {duration:.2f} 秒")
            return True
        else:
            logger.error(f"❌ {script_name} 运行失败，错误代码: {return_code}")
            return False
        
    except Exception as e:
        logger.error(f"❌ {script_name} 运行时发生异常: {str(e)}")
        return False

def check_required_files(logger):
    """检查必需的脚本文件是否存在"""
    required_files = [
        "fetch_yc_companies.py",
        "batch_scraper.py", 
        "update_jobs_with_company_info_merged.py"
    ]
    
    missing_files = []
    for file in required_files:
        if not os.path.exists(file):
            missing_files.append(file)
    
    if missing_files:
        logger.error(f"缺少必需的脚本文件: {missing_files}")
        return False
    
    logger.info("所有必需的脚本文件都存在")
    return True

def get_latest_files(pattern, logger):
    """获取匹配模式的最新文件"""
    import glob
    
    # 首先在result目录中查找
    result_pattern = os.path.join("result", pattern)
    result_files = glob.glob(result_pattern)
    
    # 然后在根目录中查找
    root_files = glob.glob(pattern)
    
    # 合并所有文件
    all_files = result_files + root_files
    
    if not all_files:
        logger.warning(f"未找到匹配模式的文件: {pattern}")
        return None
    
    latest_file = max(all_files, key=os.path.getmtime)
    logger.info(f"找到最新文件: {latest_file}")
    return latest_file

def main():
    """主函数"""
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info("🚀 开始执行完整的数据爬取和合并流程")
    logger.info("=" * 60)
    
    total_start_time = time.time()
    
    # 检查必需文件
    if not check_required_files(logger):
        logger.error("❌ 缺少必需文件，脚本退出")
        sys.exit(1)
    
    success_count = 0
    total_steps = 3
    
    # 步骤1: 爬取hiring companies数据
    logger.info("\n📋 步骤 1/3: 爬取最新的hiring companies数据")
    logger.info("-" * 50)
    if run_script("fetch_yc_companies.py", logger):
        success_count += 1
        # 检查生成的文件
        latest_companies_file = get_latest_files("all_hiring_companies_*.csv", logger)
        if latest_companies_file:
            logger.info(f"✅ 成功生成companies文件: {latest_companies_file}")
        else:
            logger.warning("⚠️ 未找到生成的companies文件")
    else:
        logger.error("❌ 步骤1失败，但继续执行后续步骤")
    
    # 步骤2: 爬取jobs数据
    logger.info("\n💼 步骤 2/3: 爬取最新的jobs数据")
    logger.info("-" * 50)
    
    # 读取companies文件获取实际公司数量
    import pandas as pd
    companies_count = 0
    try:
        latest_companies_file = get_latest_files("all_hiring_companies_*.csv", logger)
        if latest_companies_file:
            companies_df = pd.read_csv(latest_companies_file)
            companies_count = len(companies_df)
            logger.info(f"检测到 {companies_count} 家公司，将爬取前100家公司的职位数据")
        else:
            logger.warning("未找到companies文件，使用默认参数")
            companies_count = 100
    except Exception as e:
        logger.warning(f"读取companies文件失败: {e}，使用默认参数")
        companies_count = 100
    
    # 设置爬取范围，爬取所有公司
    end_index = companies_count - 1 if companies_count > 0 else 1275
    total_companies = end_index + 1
    
    # 计算预计时间（每家公司平均15秒，包括爬取时间和间隔）
    estimated_seconds = total_companies * 15
    estimated_hours = estimated_seconds // 3600
    estimated_minutes = (estimated_seconds % 3600) // 60
    
    logger.info(f"将爬取所有 {companies_count} 家公司的职位数据（索引0-{end_index}）")
    logger.info(f"预计耗时: {estimated_hours}小时{estimated_minutes}分钟 (约{estimated_seconds}秒)")
    logger.info(f"预计完成时间: {(datetime.datetime.now() + datetime.timedelta(seconds=estimated_seconds)).strftime('%Y-%m-%d %H:%M:%S')}")
    
    if run_script_with_args("batch_scraper.py", ["run", "0", str(end_index), "1"], logger):
        success_count += 1
        # 检查生成的文件
        latest_jobs_file = get_latest_files("all_jobs_*.csv", logger)
        if latest_jobs_file:
            logger.info(f"✅ 成功生成jobs文件: {latest_jobs_file}")
        else:
            logger.warning("⚠️ 未找到生成的jobs文件")
    else:
        logger.error("❌ 步骤2失败，但继续执行后续步骤")
    
    # 步骤3: 合并数据
    logger.info("\n🔗 步骤 3/3: 合并数据生成最终文件")
    logger.info("-" * 50)
    if run_script("update_jobs_with_company_info_merged.py", logger):
        success_count += 1
        # 检查生成的文件
        latest_merged_file = get_latest_files("all_jobs_company_info_*.csv", logger)
        if latest_merged_file:
            logger.info(f"✅ 成功生成最终合并文件: {latest_merged_file}")
        else:
            logger.warning("⚠️ 未找到生成的合并文件")
    else:
        logger.error("❌ 步骤3失败")
    
    # 总结
    total_end_time = time.time()
    total_duration = total_end_time - total_start_time
    
    logger.info("\n" + "=" * 60)
    logger.info("📊 执行结果总结")
    logger.info("=" * 60)
    logger.info(f"总耗时: {total_duration:.2f} 秒")
    logger.info(f"成功步骤: {success_count}/{total_steps}")
    
    if success_count == total_steps:
        logger.info("🎉 所有步骤执行成功！")
        
        # 显示最终生成的文件
        logger.info("\n📁 生成的文件:")
        latest_companies = get_latest_files("all_hiring_companies_*.csv", logger)
        latest_jobs = get_latest_files("all_jobs_*.csv", logger) 
        latest_merged = get_latest_files("all_jobs_company_info_*.csv", logger)
        
        if latest_companies:
            logger.info(f"  - Companies文件: {latest_companies}")
        if latest_jobs:
            logger.info(f"  - Jobs文件: {latest_jobs}")
        if latest_merged:
            logger.info(f"  - 最终合并文件: {latest_merged}")
            
        logger.info("\n✅ 完整流程执行成功！")
        return 0
    else:
        logger.error(f"❌ 部分步骤失败 ({success_count}/{total_steps})")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)