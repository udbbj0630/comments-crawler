#!/usr/bin/env python3
"""
Discourse 论坛用户留言抓取器
支持：自动分页、代理、断点续传、多格式导出
"""
import requests
import json
import time
import csv
import re
import sys
import os
from urllib.parse import quote
from datetime import datetime
from typing import List, Dict, Optional

class DiscourseCrawler:
    def __init__(self, base_url: str, username: str, proxy: Optional[str] = None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.proxy = {'http': proxy, 'https': proxy} if proxy else None
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'X-Requested-With': 'XMLHttpRequest',
            'Referer': f'{self.base_url}/',
        })
        
        self.posts: List[Dict] = []
        self.user_info: Dict = {}
        
    def fetch(self, url: str, params: Optional[dict] = None, retries: int = 3) -> Optional[dict]:
        for i in range(retries):
            try:
                print(f"  → {url[:60]}... (尝试 {i+1}/{retries})")
                r = self.session.get(url, params=params, proxies=self.proxy, timeout=60)
                r.raise_for_status()
                return r.json()
            except Exception as e:
                print(f"     ✗ 失败: {e}")
                if i < retries - 1:
                    time.sleep(2 ** i)
                else:
                    raise
        return None
    
    def get_user_info(self) -> bool:
        url = f"{self.base_url}/u/{quote(self.username)}.json"
        print(f"\n[1/3] 获取用户信息...")
        
        data = self.fetch(url)
        if not data or 'user' not in data:
            print("✗ 获取用户信息失败")
            return False
        
        self.user_info = data['user']
        print(f"✓ 用户名: {self.user_info.get('username')}")
        print(f"  注册时间: {self.user_info.get('created_at', 'N/A')[:10]}")
        print(f"  发帖数: {self.user_info.get('post_count', 0)}")
        print(f"  回复数: {self.user_info.get('reply_count', 0)}")
        return True
    
    def get_activity_stream(self, offset: int = 0) -> List[Dict]:
        url = f"{self.base_url}/user_actions.json"
        params = {
            'username': self.username,
            'filter': '4,5,6',
            'offset': offset
        }
        data = self.fetch(url, params)
        return data.get('user_actions', []) if data else []
    
    def crawl_all_posts(self, max_batches: int = 500):
        print(f"\n[2/3] 抓取留言中...")
        
        offset = 0
        batch = 0
        
        checkpoint_file = f"{self.username}_checkpoint.json"
        if os.path.exists(checkpoint_file):
            with open(checkpoint_file, 'r') as f:
                saved = json.load(f)
                self.posts = saved.get('posts', [])
                offset = saved.get('offset', 0)
                print(f"  从断点恢复: 已有 {len(self.posts)} 条, offset={offset}")
        
        while batch < max_batches:
            batch += 1
            actions = self.get_activity_stream(offset)
            
            if not actions:
                print(f"  数据抓取完毕")
                break
            
            for action in actions:
                if action.get('action_type') in [4, 5]:
                    post = {
                        'id': action.get('post_id'),
                        'topic_id': action.get('topic_id'),
                        'topic_title': action.get('title', ''),
                        'created_at': action.get('created_at'),
                        'post_number': action.get('post_number'),
                        'excerpt': self._clean_html(action.get('excerpt', '')),
                        'url': f"{self.base_url}/t/topic/{action.get('topic_id')}/{action.get('post_number')}",
                        'action_type': '发帖' if action.get('action_type') == 5 else '回复'
                    }
                    self.posts.append(post)
            
            print(f"  批次 {batch}: +{len(actions)} 条操作, 累计 {len(self.posts)} 条留言")
            
            if batch % 5 == 0:
                with open(checkpoint_file, 'w') as f:
                    json.dump({'posts': self.posts, 'offset': offset + len(actions)}, f, ensure_ascii=False)
            
            if len(actions) < 30:
                break
            
            offset += len(actions)
            time.sleep(0.6)
        
        if os.path.exists(checkpoint_file):
            os.remove(checkpoint_file)
        
        print(f"✓ 共抓取 {len(self.posts)} 条留言")
    
    def _clean_html(self, text: str) -> str:
        text = text.replace("<span class='mention'>@</span>", "@")
        text = re.sub(r'<[^>]+>', '', text)
        text = text.replace('&quot;', '"').replace('&amp;', '&')
        return text.strip()
    
    def export_json(self, filename: Optional[str] = None):
        if not filename:
            filename = f"{self.username}_posts.json"
        
        result = {
            'crawled_at': datetime.now().isoformat(),
            'source': self.base_url,
            'username': self.username,
            'user_info': {
                'username': self.user_info.get('username'),
                'created_at': self.user_info.get('created_at'),
                'post_count': self.user_info.get('post_count'),
                'reply_count': self.user_info.get('reply_count'),
                'trust_level': self.user_info.get('trust_level'),
            },
            'total_posts': len(self.posts),
            'posts': self.posts
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        
        print(f"  📁 JSON: {filename}")
        return filename
    
    def export_markdown(self, filename: Optional[str] = None):
        if not filename:
            filename = f"{self.username}_posts.md"
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"# {self.username} 的论坛留言\n\n")
            f.write(f"> 来源: {self.base_url}\n")
            f.write(f"> 抓取时间: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
            f.write(f"> 注册时间: {self.user_info.get('created_at', 'N/A')[:10]}\n")
            f.write(f"> 留言总数: {len(self.posts)}\n\n")
            f.write("---\n\n")
            
            for i, post in enumerate(self.posts, 1):
                f.write(f"## {i}. {post['topic_title']}\n\n")
                f.write(f"- **类型**: {post['action_type']}\n")
                f.write(f"- **时间**: {post['created_at'][:10]}\n")
                f.write(f"- **链接**: [{post['url']}]({post['url']})\n\n")
                
                if post['excerpt']:
                    f.write(f"{post['excerpt']}\n\n")
                
                f.write("---\n\n")
        
        print(f"  📄 Markdown: {filename}")
        return filename
    
    def export_csv(self, filename: Optional[str] = None):
        if not filename:
            filename = f"{self.username}_posts.csv"
        
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow(['序号', '类型', '标题', '时间', '链接', '摘要'])
            
            for i, post in enumerate(self.posts, 1):
                writer.writerow([
                    i,
                    post['action_type'],
                    post['topic_title'],
                    post['created_at'][:10],
                    post['url'],
                    post['excerpt'][:200] + '...' if len(post['excerpt']) > 200 else post['excerpt']
                ])
        
        print(f"  📊 CSV: {filename}")
        return filename
    
    def run(self):
        print("=" * 60)
        print(f"Discourse 论坛爬虫")
        print(f"目标: {self.username} @ {self.base_url}")
        if self.proxy:
            print(f"代理: {self.proxy}")
        print("=" * 60)
        
        if not self.get_user_info():
            return False
        
        self.crawl_all_posts()
        
        if not self.posts:
            print("\n✗ 没有抓取到任何留言")
            return False
        
        print(f"\n[3/3] 导出结果...")
        self.export_json()
        self.export_markdown()
        self.export_csv()
        
        print(f"\n✅ 全部完成！")
        return True


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='抓取 Discourse 论坛用户留言')
    parser.add_argument('username', help='目标用户名')
    parser.add_argument('--url', default='https://www.uscardforum.com', help='论坛地址')
    parser.add_argument('--proxy', '-p', help='代理地址')
    parser.add_argument('--output-dir', '-o', default='.', help='输出目录')
    
    args = parser.parse_args()
    
    if args.output_dir != '.':
        os.makedirs(args.output_dir, exist_ok=True)
        os.chdir(args.output_dir)
    
    crawler = DiscourseCrawler(args.url, args.username, args.proxy)
    success = crawler.run()
    
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
