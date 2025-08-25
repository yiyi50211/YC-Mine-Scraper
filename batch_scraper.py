import json
import subprocess
import time
import datetime
import os
import sys
import locale

class BatchJobScraper:
    """
    批量YCombinator职位爬虫
    读取all_hiring_companies.json中的公司信息，批量爬取职位数据
    """
    
    def __init__(self):
        today = datetime.datetime.now().strftime("%Y%m%d")
        json_filename = f"all_hiring_companies_{today}.json"
        # 从result文件夹读取公司数据文件
        self.companies_file = os.path.join("result", json_filename)
        # 使用绝对路径确保能找到脚本文件
        current_dir = os.path.dirname(os.path.abspath(__file__))
        self.scraper_script = os.path.join(current_dir, "job_scraper_improved.py")
        
        # 创建logs文件夹（如果不存在）
        self.logs_dir = os.path.join(current_dir, "logs")
        if not os.path.exists(self.logs_dir):
            os.makedirs(self.logs_dir)
            
        # 日志文件路径设置到logs文件夹下
        self.log_file = os.path.join(self.logs_dir, f"batch_scraper_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        self.base_url = "https://www.ycombinator.com/companies/"
        
    def log_message(self, message):
        """
        记录日志信息到文件和控制台
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        # 输出到控制台
        print(log_entry)
        
        # 写入日志文件
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
    
    def load_companies(self):
        """
        从all_hiring_companies.json文件中加载公司信息
        """
        try:
            if not os.path.exists(self.companies_file):
                self.log_message(f"错误: 找不到公司数据文件 {self.companies_file}")
                return []
            
            with open(self.companies_file, 'r', encoding='utf-8') as f:
                companies_data = json.load(f)
            
            # 提取公司slug信息
            companies = []
            if isinstance(companies_data, list):
                for company in companies_data:
                    if isinstance(company, dict) and 'slug' in company:
                        companies.append({
                            'slug': company['slug'],
                            'name': company.get('name', company['slug']),
                            'description': company.get('description', '')
                        })
            elif isinstance(companies_data, dict):
                # 如果是字典格式，尝试从不同的键中提取
                for key, value in companies_data.items():
                    if isinstance(value, list):
                        for company in value:
                            if isinstance(company, dict) and 'slug' in company:
                                companies.append({
                                    'slug': company['slug'],
                                    'name': company.get('name', company['slug']),
                                    'description': company.get('description', '')
                                })
                        break
            
            self.log_message(f"成功加载 {len(companies)} 家公司信息")
            return companies
            
        except Exception as e:
            self.log_message(f"加载公司数据时出错: {str(e)}")
            return []
    
    def scrape_company_jobs(self, company_slug):
        """
        爬取单个公司的职位信息
        """
        company_url = f"{self.base_url}{company_slug}"
        
        try:
            self.log_message(f"开始爬取公司: {company_slug}")
            
            # 执行爬虫脚本 - 不保存单个公司的详细日志
            system_encoding = locale.getpreferredencoding()
            
            result = subprocess.run(
                [sys.executable, self.scraper_script, company_url, "--append"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding=system_encoding,
                errors='replace',  # 替换无法解码的字符
                timeout=300  # 5分钟超时
            )
            
            # 记录执行结果
            # 记录执行结果
            if result.returncode == 0:
                self.log_message(f"[SUCCESS] 成功爬取公司 {company_slug}")
                # 只记录关键的输出信息
                if result.stdout:
                    output_lines = result.stdout.strip().split('\n')
                    key_lines = [line for line in output_lines if '找到' in line or '保存' in line or '完成' in line]
                    if key_lines:
                        self.log_message(f"   关键信息: {'; '.join(key_lines[-2:])}")  # 只记录最后2行关键信息
                return True
            else:
                self.log_message(f"[FAILED] 爬取公司 {company_slug} 失败，返回码: {result.returncode}")
                # 只记录简要的错误信息
                if result.stderr:
                    error_lines = [line for line in result.stderr.strip().split('\n') if 'error' in line.lower() or '错误' in line or 'exception' in line.lower()]
                    if error_lines:
                        error_msg = '; '.join(error_lines[:2])[:150]  # 限制错误信息长度
                        self.log_message(f"   错误信息: {error_msg}")
                return False
                
        except subprocess.TimeoutExpired:
            self.log_message(f"[TIMEOUT] 爬取公司 {company_slug} 超时")
            return False
        except Exception as e:
            self.log_message(f"[ERROR] 爬取公司 {company_slug} 时出现异常: {str(e)}")
            return False
    
    def run_batch_scraping(self, start_index=0, max_companies=None, delay_seconds=3):
        """
        执行批量爬取
        
        Args:
            start_index: 开始索引（用于断点续传）
            max_companies: 最大爬取公司数量（None表示全部）
            delay_seconds: 每次爬取之间的延迟时间（秒）
        """
        self.log_message("=" * 60)
        self.log_message("开始批量爬取YCombinator公司职位信息")
        self.log_message("=" * 60)
        
        # 加载公司数据
        companies = self.load_companies()
        if not companies:
            self.log_message("没有找到公司数据，退出程序")
            return
        
        # 确定爬取范围
        total_companies = len(companies)
        end_index = min(start_index + max_companies, total_companies) if max_companies else total_companies
        
        self.log_message(f"总公司数量: {total_companies}")
        self.log_message(f"爬取范围: {start_index} - {end_index-1}")
        self.log_message(f"每次爬取间隔: {delay_seconds} 秒")
        
        # 如果max_companies超过了总公司数量，给出提示
        if max_companies and max_companies > total_companies:
            self.log_message(f"注意: 指定的爬取数量 {max_companies} 超过了可用的公司总数 {total_companies}")
            self.log_message(f"将只爬取所有可用的 {total_companies} 家公司")
        
        # 统计信息
        success_count = 0
        failed_count = 0
        
        # 开始批量爬取
        for i in range(start_index, end_index):
            company = companies[i]
            company_slug = company['slug']
            company_name = company['name']
            
            self.log_message(f"\n进度: {i+1}/{end_index} - 处理公司: {company_name} ({company_slug})")
            
            # 爬取公司职位
            success = self.scrape_company_jobs(company_slug)
            
            if success:
                success_count += 1
            else:
                failed_count += 1
            
            # 记录当前统计
            self.log_message(f"当前统计 - 成功: {success_count}, 失败: {failed_count}")
            
            # 延迟，避免请求过于频繁
            if i < end_index - 1:  # 最后一个不需要延迟
                self.log_message(f"等待 {delay_seconds} 秒后继续...")
                time.sleep(delay_seconds)
        
        # 输出最终统计
        self.log_message("\n" + "=" * 60)
        self.log_message("批量爬取完成!")
        self.log_message(f"总计处理公司: {end_index - start_index}")
        self.log_message(f"成功爬取: {success_count}")
        self.log_message(f"失败爬取: {failed_count}")
        self.log_message(f"成功率: {success_count/(end_index-start_index)*100:.1f}%")
        self.log_message(f"主日志文件: {self.log_file}")
        self.log_message(f"所有日志保存在: {self.logs_dir}")
        self.log_message("=" * 60)
    
    def show_companies_preview(self, limit=10):
        """
        显示公司列表预览
        """
        companies = self.load_companies()
        if not companies:
            return
        
        self.log_message(f"\n公司列表预览 (前{min(limit, len(companies))}家):")
        self.log_message("-" * 50)
        
        for i, company in enumerate(companies[:limit]):
            self.log_message(f"{i+1:3d}. {company['name']} ({company['slug']})")
        
        if len(companies) > limit:
            self.log_message(f"... 还有 {len(companies) - limit} 家公司")
        
        self.log_message(f"\n总计: {len(companies)} 家公司")


def main():
    """
    主函数
    """
    scraper = BatchJobScraper()
    
    # 检查命令行参数
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "preview":
            # 显示公司列表预览
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            scraper.show_companies_preview(limit)
            
        elif command == "run":
            # 执行批量爬取
            start_index = int(sys.argv[2]) if len(sys.argv) > 2 else 0
            max_companies = int(sys.argv[3]) if len(sys.argv) > 3 else None
            delay_seconds = int(sys.argv[4]) if len(sys.argv) > 4 else 3
            
            # 先加载公司数据，获取总数
            companies = scraper.load_companies()
            total_companies = len(companies) if companies else 0
            
            if total_companies > 0:
                if max_companies is None:
                    print(f"将爬取从索引 {start_index} 开始的所有公司 (总计 {total_companies} 家)")
                    print(f"如果只想爬取部分公司，请指定数量，例如: python batch_scraper.py run {start_index} 10 {delay_seconds}")
                else:
                    actual_count = min(max_companies, total_companies - start_index)
                    print(f"将爬取从索引 {start_index} 开始的 {actual_count} 家公司 (总计 {total_companies} 家)")
                
                # 询问用户是否继续
                confirm = input("是否继续爬取? (y/n): ")
                if confirm.lower() != 'y':
                    print("已取消爬取")
                    return
            
            scraper.run_batch_scraping(start_index, max_companies, delay_seconds)
            
        elif command == "count":
            # 显示公司总数
            companies = scraper.load_companies()
            total_companies = len(companies) if companies else 0
            print(f"当前可用公司总数: {total_companies}")
            
        elif command == "help":
            print("批量YCombinator职位爬虫使用说明:")
            print("python batch_scraper.py preview [数量]           - 预览公司列表")
            print("python batch_scraper.py count                   - 显示可用公司总数")
            print("python batch_scraper.py run [起始索引] [最大数量] [延迟秒数] - 执行批量爬取")
            print("python batch_scraper.py help                    - 显示帮助信息")
            print("\n示例:")
            print("python batch_scraper.py preview 20              - 预览前20家公司")
            print("python batch_scraper.py run 0 10 5              - 从第0家开始爬取10家公司，每次间隔5秒")
            print("python batch_scraper.py run 10                  - 从第10家开始爬取所有剩余公司")
            
        else:
            print(f"未知命令: {command}")
            print("使用 'python batch_scraper.py help' 查看帮助信息")
    else:
        # 默认显示预览
        scraper.show_companies_preview()


if __name__ == "__main__":
    main()