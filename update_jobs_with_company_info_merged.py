import pandas as pd
import os
import glob
from datetime import datetime

def find_latest_file(pattern):
    """找到匹配模式的最新文件"""
    # 首先在result目录中查找
    result_pattern = os.path.join("result", pattern)
    result_files = glob.glob(result_pattern)
    
    # 然后在根目录中查找
    root_files = glob.glob(pattern)
    
    # 合并所有文件
    all_files = result_files + root_files
    
    if not all_files:
        return None
    
    # 按修改时间排序，返回最新的文件
    latest_file = max(all_files, key=os.path.getmtime)
    return latest_file

def update_jobs_with_company_info():
    """根据company字段关联更新jobs文件"""
    
    # 找到最新的all_jobs_文件
    jobs_pattern = "all_jobs_*.csv"
    latest_jobs_file = find_latest_file(jobs_pattern)
    
    if not latest_jobs_file:
        print("未找到all_jobs_*.csv文件")
        return
    
    # 找到最新的all_hiring_companies_文件
    companies_pattern = "all_hiring_companies_*.csv"
    latest_companies_file = find_latest_file(companies_pattern)
    
    if not latest_companies_file:
        print("未找到all_hiring_companies_*.csv文件")
        return
    
    print(f"正在处理文件:")
    print(f"Jobs文件: {latest_jobs_file}")
    print(f"Companies文件: {latest_companies_file}")
    
    try:
        # 读取jobs文件 - 尝试不同的编码
        print("正在读取jobs文件...")
        try:
            jobs_df = pd.read_csv(latest_jobs_file, encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8编码失败，尝试使用GBK编码...")
            try:
                jobs_df = pd.read_csv(latest_jobs_file, encoding='gbk')
            except UnicodeDecodeError:
                print("GBK编码失败，尝试使用latin-1编码...")
                jobs_df = pd.read_csv(latest_jobs_file, encoding='latin-1')
        print(f"Jobs文件包含 {len(jobs_df)} 条记录")
        
        # 读取companies文件 - 尝试不同的编码
        print("正在读取companies文件...")
        try:
            companies_df = pd.read_csv(latest_companies_file, encoding='utf-8')
        except UnicodeDecodeError:
            print("UTF-8编码失败，尝试使用GBK编码...")
            try:
                companies_df = pd.read_csv(latest_companies_file, encoding='gbk')
            except UnicodeDecodeError:
                print("GBK编码失败，尝试使用latin-1编码...")
                companies_df = pd.read_csv(latest_companies_file, encoding='latin-1')
        print(f"Companies文件包含 {len(companies_df)} 条记录")
        
        # 显示两个文件的列名
        print(f"\nJobs文件列名: {list(jobs_df.columns)}")
        print(f"Companies文件列名: {list(companies_df.columns)}")
        
        # 检查company字段是否存在
        if 'company' not in jobs_df.columns:
            print("错误: jobs文件中没有找到'company'字段")
            return
        
        # 假设companies文件中也有company字段或类似的字段
        company_col_in_companies = None
        possible_company_cols = ['company', 'name', 'company_name', 'Company', 'Name']
        
        for col in possible_company_cols:
            if col in companies_df.columns:
                company_col_in_companies = col
                break
        
        if not company_col_in_companies:
            print("错误: companies文件中没有找到公司名称字段")
            print(f"可用字段: {list(companies_df.columns)}")
            return
        
        print(f"使用companies文件中的'{company_col_in_companies}'字段进行匹配")
        
        # 备份原始jobs文件
        backup_file = latest_jobs_file.replace('.csv', '_backup.csv')
        jobs_df.to_csv(backup_file, index=False)
        print(f"已创建备份文件: {backup_file}")
        
        # 执行左连接，保留所有jobs记录
        print("正在执行数据关联...")
        merged_df = jobs_df.merge(
            companies_df, 
            left_on='company', 
            right_on=company_col_in_companies, 
            how='left',
            suffixes=('', '_company')
        )
        
        # 将id字段重命名为company_id
        if 'id' in merged_df.columns:
            merged_df = merged_df.rename(columns={'id': 'company_id'})
            print("已将'id'字段重命名为'company_id'")
        
        print(f"关联后包含 {len(merged_df)} 条记录")
        
        # 统计匹配情况
        matched_count = merged_df[company_col_in_companies].notna().sum()
        unmatched_count = len(merged_df) - matched_count
        
        print(f"匹配成功: {matched_count} 条记录")
        print(f"未匹配: {unmatched_count} 条记录")
        
        # 如果有重复的列名，处理冲突
        duplicate_cols = []
        current_columns = set(merged_df.columns)
        
        for col in merged_df.columns:
            if col.endswith('_company'):
                original_col = col[:-8]
                # 检查原始列是否存在（排除已重命名的id字段）
                if original_col in jobs_df.columns or (original_col == 'id' and 'company_id' in current_columns):
                    duplicate_cols.append(col)
        
        if duplicate_cols:
            print(f"发现重复字段: {duplicate_cols}")
            print("将使用companies文件中的数据覆盖jobs文件中的对应字段")
            
            for col in duplicate_cols:
                original_col = col[:-8]
                
                # 特殊处理id字段，因为已经重命名为company_id
                if original_col == 'id':
                    # 删除重复的id_company列，因为我们已经有company_id了
                    merged_df = merged_df.drop(columns=[col])
                    continue
                
                # 检查原始列是否存在
                if original_col in merged_df.columns:
                    # 用companies文件的数据填充空值或覆盖原有数据
                    merged_df[original_col] = merged_df[col].fillna(merged_df[original_col])
                
                # 删除重复列
                merged_df = merged_df.drop(columns=[col])
        
        # 移除不需要的name字段（因为已经有company字段了）
        if 'name' in merged_df.columns:
            merged_df = merged_df.drop(columns=['name'])
            print("已移除重复的'name'字段")
        
        # 检查并移除重复的company_id字段
        if merged_df.columns.tolist().count('company_id') > 1:
            # 保留第一个company_id，移除重复的
            cols = merged_df.columns.tolist()
            first_company_id_idx = cols.index('company_id')
            duplicate_indices = [i for i, col in enumerate(cols) if col == 'company_id' and i != first_company_id_idx]
            
            for idx in sorted(duplicate_indices, reverse=True):
                merged_df = merged_df.drop(columns=[cols[idx]])
            print("已移除重复的'company_id'字段")
        
        # 处理可能的company_id.1等重命名问题
        if 'company_id.1' in merged_df.columns:
            merged_df = merged_df.rename(columns={'company_id.1': 'company_id'})
            print("已将'company_id.1'重命名为'company_id'")
        
        # 移除不需要的字段
        fields_to_remove = ['isHiring', 'url', 'api']
        removed_fields = []
        for field in fields_to_remove:
            if field in merged_df.columns:
                merged_df = merged_df.drop(columns=[field])
                removed_fields.append(field)
        
        if removed_fields:
            print(f"已移除字段: {removed_fields}")
        
        # 确保result目录存在
        result_dir = "result"
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        
        # 生成带日期和时间的输出文件名，避免文件冲突
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f"all_jobs_company_info_{current_datetime}.csv"
        output_file = os.path.join(result_dir, output_filename)
        
        # 保存最终文件
        merged_df.to_csv(output_file, index=False)
        
        print(f"已保存最终文件: {output_file}")
        
        # 显示新增的字段
        new_columns = set(merged_df.columns) - set(jobs_df.columns)
        if new_columns:
            print(f"新增字段: {list(new_columns)}")
        
        # 显示一些统计信息
        print(f"\n更新后文件统计:")
        print(f"总记录数: {len(merged_df)}")
        print(f"总字段数: {len(merged_df.columns)}")
        print(f"字段列表: {list(merged_df.columns)}")
        
        return output_file
        
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
        return None

if __name__ == "__main__":
    print("开始更新jobs文件...")
    result = update_jobs_with_company_info()
    
    if result:
        print(f"\n处理完成! 输出文件: {result}")
    else:
        print("\n处理失败!")
