import requests
from bs4 import BeautifulSoup
import re
import json
import csv
import sys
import time
import os
import datetime

class YCombinatorJobScraper:
    """
    YCombinator公司职位爬虫
    支持根据输入的公司URL爬取所有职位信息
    """
    
    def __init__(self):
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        self.base_url = "https://www.ycombinator.com"
        self.apply_base_url = "https://www.workatastartup.com/jobs/"
    
    def get_company_name(self, url):
        """
        从URL中提取公司名称
        """
        # 从URL中提取公司名称
        company_match = re.search(r'/companies/([^/]+)', url)
        if company_match:
            return company_match.group(1).capitalize()
        return "未知公司"
    
    def fetch_page(self, url, max_retries=3, retry_delay=2):
        """
        获取页面内容，带有重试机制
        """
        retries = 0
        while retries < max_retries:
            try:
                print(f"正在获取页面: {url} (尝试 {retries + 1}/{max_retries})")
                response = requests.get(url, headers=self.headers, timeout=10)
                response.raise_for_status()
                return response.text
            except requests.exceptions.Timeout:
                print(f"请求超时，正在重试...")
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 504:
                    print(f"服务器网关超时 (504)，正在重试...")
                else:
                    print(f"HTTP错误: {e}")
                    return None
            except Exception as e:
                print(f"获取页面时出错: {str(e)}")
                return None
            
            retries += 1
            if retries < max_retries:
                print(f"等待 {retry_delay} 秒后重试...")
                time.sleep(retry_delay)
                # 每次重试增加延迟时间
                retry_delay *= 1.5
        
        print(f"达到最大重试次数 ({max_retries})，无法获取页面")
        return None
    
    def parse_job_links(self, html, company_url):
        """
        从公司职位页面解析所有职位链接
        """
        if not html:
            return []
            
        soup = BeautifulSoup(html, 'html.parser')
        company_name = self.get_company_name(company_url)
        job_links = []
        
        # 方法1: 查找所有职位卡片
        print("正在查找职位链接...")
        job_cards = soup.select('div.job')
        
        if job_cards:
            print(f"找到 {len(job_cards)} 个职位卡片")
            for card in job_cards:
                try:
                    title_element = card.select_one('h4.job-title')
                    link_element = card.select_one('a')
                    
                    if title_element and link_element and 'href' in link_element.attrs:
                        title = title_element.text.strip()
                        link = link_element['href']
                        if not link.startswith('http'):
                            link = self.base_url + link
                        
                        job_links.append({
                            "company": company_name,
                            "job_name": title,
                            "link_url": link
                        })
                        print(f"找到职位: {title} -> {link}")
                except Exception as e:
                    print(f"解析职位卡片时出错: {str(e)}")
        
        # 方法2: 查找所有可能的职位链接
        if not job_links:
            print("尝试查找所有可能的职位链接...")
            links = soup.find_all('a', href=re.compile(rf'/companies/{company_name.lower()}/jobs/[^/]+$', re.IGNORECASE))
            
            for link in links:
                try:
                    href = link['href']
                    # 尝试找到职位标题
                    title = None
                    
                    # 尝试从链接文本中获取标题
                    if link.text.strip():
                        title = link.text.strip()
                    
                    # 尝试从附近的h4元素获取标题
                    if not title:
                        parent = link.parent
                        for _ in range(3):  # 向上查找3层
                            if parent:
                                title_elem = parent.find('h4')
                                if title_elem:
                                    title = title_elem.text.strip()
                                    break
                                parent = parent.parent
                    
                    # 如果仍然没有标题，从URL中提取
                    if not title:
                        title_match = re.search(r'/jobs/[^-]+-(.+)$', href)
                        if title_match:
                            title = title_match.group(1).replace('-', ' ').title()
                        else:
                            title = "未命名职位"
                    
                    if not href.startswith('http'):
                        href = self.base_url + href
                    
                    job_links.append({
                        "company": company_name,
                        "job_name": title,
                        "link_url": href
                    })
                    print(f"找到职位: {title} -> {href}")
                except Exception as e:
                    print(f"解析职位链接时出错: {str(e)}")
        
        return job_links
    
    @staticmethod
    def parse_range_simple(s):
        """
        从字符串中提取数字范围
        例如: "$100K - $165K" -> [100, 165]
        """
        # 只保留数字、小数点和 -
        cleaned = re.sub(r'[^0-9\.\-]', '', s)
        parts = cleaned.split('-')
        numbers = [float(p) for p in parts if p]  # 去掉空值并转成浮点数
        return numbers
    
    def extract_job_details(self, job_url):
        """
        从职位详情页面提取job_id和其他详细信息
        """
        # 从URL中提取职位标识符
        job_identifier = job_url.split('/')[-1]
        
        # 从页面中提取job_id和其他信息
        html = self.fetch_page(job_url)
        if not html:
            return None, {}, None
        
        # 提取job_id
        job_id = None
        # 从页面中查找signup_job_id，考虑URL编码的情况
        signup_job_id_patterns = [
            r'signup_job_id=(\d+)',           # 标准格式
            r'signup_job_id%3D(\d+)',         # URL编码格式 %3D 是等号的编码
            r'signup_job_id%253D(\d+)'        # 双重URL编码格式
        ]
        
        for pattern in signup_job_id_patterns:
            signup_job_id_matches = re.findall(pattern, html)
            if signup_job_id_matches:
                job_id = signup_job_id_matches[0]
                print(f"使用模式 '{pattern}' 找到signup_job_id: {job_id}")
                break
        
        # 初始化职位详情
        job_details = {
            "salaryRange": "",
            "equityRange": "",
            "minExperience": "",
            "minSchoolYear": "",
            "visa": "",
            "jobType": "",
            "location": "",
            "hiring_description": "",
            "job_description": "",
            "salary_min": "",
            "salary_max": "",
            "experience_years": ""
        }
        
        # 直接从页面源码中提取结构化的职位信息
        # 查找包含所有字段的JSON结构（包括新增的jobType和location）
        json_pattern = r'salaryRange&quot;:&quot;(.*?)&quot;,&quot;equityRange&quot;:&quot;(.*?)&quot;,&quot;minExperience&quot;:&quot;(.*?)&quot;,&quot;minSchoolYear&quot;:&quot;(.*?)&quot;,&quot;visa&quot;:&quot;(.*?)&quot;'
        json_match = re.search(json_pattern, html)
        
        # 从HTML结构中提取Location和Job Type
        # 格式: <div><strong>Location</strong></div><span>San Francisco, CA </span>
        location_pattern = r'<div><strong>Location</strong></div><span>(.*?)</span>'
        location_match = re.search(location_pattern, html)
        if location_match:
            job_details["location"] = location_match.group(1).strip()
            print(f"找到工作地点: '{job_details['location']}'")
        
        # 格式: <div><strong>Job Type</strong></div><span>Full-time</span>
        job_type_pattern = r'<div><strong>Job Type</strong></div><span>(.*?)</span>'
        job_type_match = re.search(job_type_pattern, html)
        if job_type_match:
            job_details["jobType"] = job_type_match.group(1).strip()
            print(f"找到职位类型: '{job_details['jobType']}'")
        
        # 提取hiring_description字段
        hiring_description_pattern = r'hiring_description&quot;:&quot;(.*?)&quot;'
        hiring_description_match = re.search(hiring_description_pattern, html)
        if hiring_description_match:
            job_details["hiring_description"] = hiring_description_match.group(1)
            print(f"找到招聘描述: '{job_details['hiring_description'][:100]}...'")
        
        # 提取job_description字段（从meta标签中）
        job_description_pattern = r'<meta[^>]*content="([^"]*)"[^>]*name="description"[^>]*>'
        job_description_match = re.search(job_description_pattern, html, re.DOTALL)
        if job_description_match:
            content = job_description_match.group(1)
            # 检查内容是否以"Job Description"开头
            if content.startswith("Job Description"):
                # 提取"Job Description"之后的内容
                job_details["job_description"] = content[len("Job Description"):].strip()
                print(f"找到职位描述: '{job_details['job_description'][:100]}...'")
        
        if json_match:
            print("从页面源码中找到结构化的职位信息")
            salaryRange = json_match.group(1)
            salary_min, salary_max = '', ''
            if salaryRange:
                # 直接提取所有数字，不管是否有K后缀
                salary_matches = self.parse_range_simple(salaryRange)
                if len(salary_matches) >= 2:
                    salary_min, salary_max = salary_matches[0], salary_matches[1]
                elif len(salary_matches) == 1:
                    # 如果只找到一个值，则最小值和最大值相同
                    salary_min = salary_max = salary_matches[0]
            job_details["salaryRange"] = salaryRange
            job_details["salary_min"] = str(salary_min) if salary_min != '' else ''
            job_details["salary_max"] = str(salary_max) if salary_max != '' else ''
            job_details["equityRange"] = json_match.group(2)
            minExperience = json_match.group(3)
            experience_years = ""
            if minExperience:
                # 将"Any (new grads ok)"替换为"0"
                if minExperience == "Any (new grads ok)":
                    minExperience = "0"
                    experience_years = "0"
                else:
                    #提取minExperience中的数字，如6+ years
                    experience_match = re.search(r'(\d+)', minExperience)
                    if experience_match:
                        experience_years = experience_match.group(1)
                        minExperience = experience_years
            job_details["minExperience"] = str(minExperience) if minExperience else ""
            job_details["experience_years"] = experience_years
            #提取
            job_details["minSchoolYear"] = json_match.group(4)
            job_details["visa"] = json_match.group(5)
            
            # 打印提取到的信息
            print(f"薪资范围: '{job_details['salaryRange']}'")
            print(f"股权范围: '{job_details['equityRange']}'")
            print(f"最低经验: '{job_details['minExperience']}'")
            print(f"最低学历: '{job_details['minSchoolYear']}'")
            print(f"签证支持: '{job_details['visa']}'")
            
            # 直接返回提取到的信息，不进行任何推断或默认值设置
            return job_id, job_details, html
        
        # 如果没有找到结构化数据，直接返回空值，不进行推断
        print("未找到结构化的职位信息，返回空值")
        return job_id, job_details, html
    
    def scrape_jobs(self, company_url):
        """
        爬取公司的所有职位信息
        """
        # 确保URL以/jobs结尾
        if not company_url.endswith('/jobs'):
            company_url = company_url.rstrip('/') + '/jobs'
        
        # 获取公司职位页面
        html = self.fetch_page(company_url)
        if not html:
            print(f"无法获取公司职位页面: {company_url}")
            return []
        
        # 解析职位链接
        job_links = self.parse_job_links(html, company_url)
        if not job_links:
            print(f"未在 {company_url} 找到任何职位")
            return []
        
        # 获取每个职位的详细信息
        print(f"\n正在获取每个职位的详细信息...")
        for job in job_links:
            try:
                print(f"\n处理职位: {job['job_name']}")
                job_id, job_details, _ = self.extract_job_details(job['link_url'])
                
                if job_id:
                    job['job_id'] = job_id
                    job['apply_url'] = f"{self.apply_base_url}{job_id}"
                else:
                    job['job_id'] = "未知"
                    job['apply_url'] = "未找到申请链接"
                
                # 添加职位详情
                job['salaryRange'] = job_details.get('salaryRange', '')
                job['equityRange'] = job_details.get('equityRange', '')
                job['minExperience'] = job_details.get('minExperience', '')
                job['minSchoolYear'] = job_details.get('minSchoolYear', '')
                job['visa'] = job_details.get('visa', '')
                job['jobType'] = job_details.get('jobType', '')
                job['location'] = job_details.get('location', '')
                # 添加is_remote字段，根据location是否包含"Remote"来判断
                job['is_remote'] = 'Remote' in job_details.get('location', '')
                job['hiring_description'] = job_details.get('hiring_description', '')
                job['job_description'] = job_details.get('job_description', '')
                # 添加提取的数值字段
                job['salary_min'] = job_details.get('salary_min', '')
                job['salary_max'] = job_details.get('salary_max', '')
                
                # 添加延迟，避免请求过于频繁
                time.sleep(2)
            except Exception as e:
                print(f"处理职位 {job['job_name']} 时出错: {str(e)}")
                job['job_id'] = "错误"
                job['apply_url'] = "处理时出错"
                job['salaryRange'] = ""
                job['equityRange'] = ""
                job['minExperience'] = ""
                job['minSchoolYear'] = ""
                job['visa'] = ""
                job['jobType'] = ""
                job['location'] = ""
                job['hiring_description'] = ""
                job['job_description'] = ""
        
        return job_links
    
    def save_to_csv(self, jobs, filename=None, append_mode=False):
        """
        将职位数据保存为CSV文件（支持追加模式，文件名包含日期）
        """
        if not jobs:
            print("没有职位数据可保存")
            return
        
        # 确保result目录存在
        result_dir = "result"
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        
        # 生成带日期的文件名
        if filename is None:
            today = datetime.datetime.now().strftime("%Y%m%d")
            filename = f"all_jobs_{today}.csv"
        
        # 添加result路径前缀
        filepath = os.path.join(result_dir, filename)
        
        # 检查文件是否存在以及是否需要写入表头
        file_exists = os.path.exists(filepath)
        write_header = not file_exists or not append_mode
        
        # 根据模式选择写入方式
        mode = 'a' if append_mode and file_exists else 'w'
        
        with open(filepath, mode, newline='', encoding='utf-8') as csvfile:
            fieldnames = ['company', 'job_name', 'link_url', 'apply_url', 'job_id', 
                         'salaryRange', 'equityRange', 'minExperience', 'minSchoolYear', 'visa', 'jobType', 'location', 'is_remote',
                         'hiring_description', 'job_description', 'salary_min', 'salary_max']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # 只在需要时写入表头
            if write_header:
                writer.writeheader()
            
            writer.writerows(jobs)
        
        action = "追加到" if append_mode and file_exists else "保存到"
        print(f"数据已成功{action} {filepath}，新增 {len(jobs)} 条记录")
        return filepath
    
    def save_to_json(self, jobs, company_name, append_mode=False):
        """
        将职位数据保存为JSON文件（支持追加模式，文件名包含日期）
        """
        if not jobs:
            print("没有职位数据可保存")
            return
        
        # 确保result目录存在
        result_dir = "result"
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        
        today = datetime.datetime.now().strftime("%Y%m%d")
        
        # 1. 保存到主文件（带日期）
        main_filename = f"all_jobs_{today}.json"
        main_filepath = os.path.join(result_dir, main_filename)
        
        if append_mode and os.path.exists(main_filepath):
            # 追加模式：读取现有数据，合并后写入
            try:
                with open(main_filepath, 'r', encoding='utf-8') as jsonfile:
                    existing_jobs = json.load(jsonfile)
                if not isinstance(existing_jobs, list):
                    existing_jobs = []
            except (json.JSONDecodeError, FileNotFoundError):
                existing_jobs = []
            
            # 合并数据
            all_jobs = existing_jobs + jobs
            
            with open(main_filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(all_jobs, jsonfile, ensure_ascii=False, indent=2)
            
            print(f"数据已成功追加到 {main_filepath}，新增 {len(jobs)} 条记录，总计 {len(all_jobs)} 条记录")
        else:
            # 覆盖模式或文件不存在
            with open(main_filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(jobs, jsonfile, ensure_ascii=False, indent=2)
            
            print(f"数据已成功保存到 {main_filepath}，共 {len(jobs)} 条记录")
        
        # 2. 保存单公司结果到result/jobs_info文件夹
        jobs_info_dir = os.path.join(result_dir, "jobs_info")
        if not os.path.exists(jobs_info_dir):
            os.makedirs(jobs_info_dir)
            print(f"创建目录: {jobs_info_dir}")
        
        company_filename = f"{company_name.lower()}_{today}.json"
        company_filepath = os.path.join(jobs_info_dir, company_filename)
        with open(company_filepath, 'w', encoding='utf-8') as jsonfile:
            json.dump(jobs, jsonfile, ensure_ascii=False, indent=2)
        
        print(f"单公司数据已保存到 {company_filepath}，共 {len(jobs)} 条记录")
        
        return main_filepath, company_filepath
    
    def print_jobs(self, jobs):
        """
        打印职位数据
        """
        if not jobs:
            print("没有找到职位数据")
            return
            
        print(f"\n找到 {len(jobs)} 个职位:")
        for job in jobs:
            print(f"公司: {job['company']}")
            print(f"职位: {job['job_name']}")
            print(f"链接: {job['link_url']}")
            print(f"申请链接: {job['apply_url']}")
            print(f"职位ID: {job['job_id']}")
            print(f"薪资范围: {job['salaryRange']}")
            print(f"薪资下限: {job.get('salary_min', '')}")
            print(f"薪资上限: {job.get('salary_max', '')}")
            print(f"股权范围: {job['equityRange']}")
            print(f"最低经验: {job['minExperience']}")
            print(f"最低学历: {job['minSchoolYear']}")
            print(f"签证支持: {job['visa']}")
            print(f"职位类型: {job['jobType']}")
            print(f"工作地点: {job['location']}")
            print(f"远程工作: {'是' if job.get('is_remote', False) else '否'}")
            print(f"招聘描述: {job.get('hiring_description', '')[:100]}...")
            print(f"职位描述: {job.get('job_description', '')[:100]}...")
            print("-" * 50)


def main():
    """
    主函数
    """
    try:
        # 检查命令行参数
        append_mode = False
        if len(sys.argv) > 1:
            company_url = sys.argv[1]
            # 检查是否有--append参数
            if len(sys.argv) > 2 and sys.argv[2] == "--append":
                append_mode = True
                print("启用追加模式")
        else:
            # 默认使用Bree公司的URL进行测试
            company_url = "https://www.ycombinator.com/companies/bree"
        
        # 创建爬虫实例
        scraper = YCombinatorJobScraper()
        
        # 爬取职位信息
        print(f"开始爬取 {company_url} 的职位信息...")
        jobs = scraper.scrape_jobs(company_url)
        
        # 保存结果
        if jobs:
            company_name = scraper.get_company_name(company_url)
            
            # 保存到带日期的CSV文件（支持追加模式）
            csv_filename = scraper.save_to_csv(jobs, filename=None, append_mode=append_mode)
            
            # 保存到带日期的JSON文件（支持追加模式）
            main_json_filename, company_json_filename = scraper.save_to_json(jobs, company_name, append_mode=append_mode)
            
            # 打印职位信息
            scraper.print_jobs(jobs)
            
            print(f"\n爬取完成! 共找到 {len(jobs)} 个职位。")
            print(f"结果已保存到:")
            print(f"  - 主CSV文件: {csv_filename}")
            print(f"  - 主JSON文件: {main_json_filename}")
            print(f"  - 单公司JSON文件: {company_json_filename}")
        else:
            print("未找到任何职位信息")
    
    except KeyboardInterrupt:
        print("\n用户中断爬取过程")
    except Exception as e:
        print(f"爬取过程中出现错误: {str(e)}")


if __name__ == "__main__":
    main()