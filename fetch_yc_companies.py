#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YC公司数据获取脚本
从 https://yc-oss.github.io/api/companies/all.json 获取所有YC公司信息
保存为JSON和CSV格式，文件名包含当日日期
"""

import requests
import json
import csv
import pandas as pd
from datetime import datetime
import logging
import sys
import os

# 创建logs文件夹（如果不存在）
os.makedirs('logs', exist_ok=True)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/yc_companies_fetch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)

def fetch_yc_companies():
    """
    从YC API获取所有公司数据
    """
    #api_url = "https://yc-oss.github.io/api/companies/all.json"
    api_url = "https://yc-oss.github.io/api/companies/hiring.json"
    try:
        logger.info(f"开始从 {api_url} 获取数据...")
        
        # 发送GET请求
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        # 解析JSON数据
        companies_data = response.json()
        
        logger.info(f"成功获取 {len(companies_data)} 家公司的数据")
        return companies_data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"请求失败: {e}")
        return None
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}")
        return None
    except Exception as e:
        logger.error(f"未知错误: {e}")
        return None

def save_to_json(data, filename):
    """
    保存数据为JSON格式
    """
    try:
        # 确保result目录存在
        result_dir = "result"
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        
        # 添加result路径前缀
        filepath = os.path.join(result_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"成功保存JSON文件: {filepath}")
        return True
    except Exception as e:
        logger.error(f"保存JSON文件失败: {e}")
        return False

def save_to_csv(data, filename):
    """
    保存数据为CSV格式
    """
    try:
        if not data:
            logger.warning("没有数据可保存")
            return False
        
        # 确保result目录存在
        result_dir = "result"
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        
        # 添加result路径前缀
        filepath = os.path.join(result_dir, filename)
            
        # 使用pandas处理数据
        df = pd.json_normalize(data)
        
        # 保存为CSV
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"成功保存CSV文件: {filepath} (共 {len(df)} 行数据)")
        return True
        
    except Exception as e:
        logger.error(f"保存CSV文件失败: {e}")
        return False

def get_file_names():
    """
    生成带日期的文件名
    """
    today = datetime.now().strftime("%Y%m%d")
    #json_filename = f"all_launched_companies_{today}.json"
    #csv_filename = f"all_launched_companies_{today}.csv"
    json_filename = f"all_hiring_companies_{today}.json"
    csv_filename = f"all_hiring_companies_{today}.csv"
    return json_filename, csv_filename

def main():
    """
    主函数
    """
    logger.info("=== YC公司数据获取脚本开始执行 ===")
    
    # 获取数据
    companies_data = fetch_yc_companies()
    
    if companies_data is None:
        logger.error("获取数据失败，脚本退出")
        sys.exit(1)
    
    # 生成文件名
    json_filename, csv_filename = get_file_names()
    
    # 保存JSON文件
    json_success = save_to_json(companies_data, json_filename)
    
    # 保存CSV文件
    csv_success = save_to_csv(companies_data, csv_filename)
    
    # 输出结果统计
    logger.info("=== 执行结果统计 ===")
    logger.info(f"获取公司数量: {len(companies_data)}")
    logger.info(f"JSON文件保存: {'成功' if json_success else '失败'}")
    logger.info(f"CSV文件保存: {'成功' if csv_success else '失败'}")
    
    if json_success and csv_success:
        logger.info("所有文件保存成功！")
        logger.info(f"JSON文件: {json_filename}")
        logger.info(f"CSV文件: {csv_filename}")
    else:
        logger.error("部分文件保存失败")
        sys.exit(1)
    
    logger.info("=== 脚本执行完成 ===")

if __name__ == "__main__":
    main()