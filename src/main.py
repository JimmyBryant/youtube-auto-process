"""
视频处理系统主入口 (增强版)

新增功能：
1. 完整的任务创建实现
2. 输入验证
3. 任务进度反馈
4. 错误处理机制
"""

import asyncio
import argparse
import sys
from typing import Optional, Dict
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from src.core.models import TaskModel
from src.core.task_manager import TaskManager
from src.core.task_scheduler import TaskScheduler
from src import __version__, get_version

# 环境配置加载
load_dotenv(Path(__file__).parent.parent / 'config' / 'dev.env')

class TaskCLI:
    @staticmethod
    async def delete_task(task_id: str):
        """删除指定任务"""
        manager = TaskManager()
        ok = await manager.delete_task(task_id)
        if ok:
            print(f"✅ 任务已删除 | ID: {task_id}")
        else:
            print(f"❌ 未找到或删除失败 | ID: {task_id}")
    @staticmethod
    async def show_tasks_detail(n: int):
        """显示前 n 个任务的所有阶段详情"""
        manager = TaskManager()
        tasks = await manager.list_tasks(limit=n)
        if not tasks:
            print("ℹ️ 没有找到任务")
            return
        for idx, task in enumerate(tasks, 1):
            print(f"\n任务 #{idx} | ID: {task.id}")
            print(f"  状态: {task.status} | 优先级: {task.priority}")
            print(f"  URL: {task.video_url}")
            print(f"  创建时间: {task.timestamps.get('created_at')}")
            print(f"  阶段进度:")
            for stage in task.stage_progress:
                progress = task.stage_progress[stage]
                # StageProgress 是 pydantic model，直接用属性
                stage_status = getattr(progress, 'status', '-')
                started = getattr(progress, 'started_at', None)
                completed = getattr(progress, 'completed_at', None)
                print(f"    - {stage}: {stage_status}")
                if started:
                    print(f"      开始: {started}")
                if completed:
                    print(f"      完成: {completed}")
                if hasattr(progress, 'output_files') and progress.output_files:
                    print(f"      输出文件: {progress.output_files}")
                if getattr(progress, 'error', None):
                    print(f"      错误: {progress.error}")
            print(f"  {'='*40}")
    """任务命令行交互处理器"""
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """验证视频URL格式"""
        return url.startswith(('http://', 'https://')) and any(
            domain in url for domain in ['youtube.com', 'youtu.be']
        )

    @staticmethod
    async def create_task(video_url: str, priority: int = 5) -> str:
        """
        创建新任务 (完整实现)
        
        参数:
            video_url: 视频URL (必须包含youtube域名)
            priority: 优先级 (1-9, 默认5)
            
        返回:
            任务ID
            
        异常:
            ValueError: 当输入无效时
            RuntimeError: 当任务创建失败时
        """
        if not TaskCLI.validate_url(video_url):
            raise ValueError("❌ 无效的视频URL，必须是YouTube链接")
        
        if not 1 <= priority <= 9:
            raise ValueError("❌ 优先级必须在1-9之间")

        manager = TaskManager()
        task_data = {
            "video_url": video_url,
            "priority": priority,
            "metadata": {
                "source": "cli",
                "quality": "1080p"  # 默认配置
            }
        }
        
        try:
            print("🔄 正在创建任务...")
            task_id = await manager.create_task(**task_data)
            print(f"✅ 任务创建成功 | ID: {task_id}")
            return task_id
        except Exception as e:
            raise RuntimeError(f"❌ 任务创建失败: {str(e)}")

    @staticmethod
    async def list_tasks(status: Optional[str] = None, limit: int = 10):
        """增强版任务列表查询"""
        manager = TaskManager()
        tasks = await manager.list_tasks(status, limit)
        
        if not tasks:
            print("ℹ️ 没有找到匹配的任务")
            return

        print(f"\n📋 任务列表 (显示 {len(tasks)}/{limit} 项)")
        for task in tasks:
            status_icon = {
                "pending": "🕒",
                "processing": "⚙️",
                "completed": "✅",
                "failed": "❌"
            }.get(task.status, "❓")
            
            print(
                f"{status_icon} [{task.id[:8]}] {task.status.upper()}\n"
                f"   URL: {task.video_url}\n"
                f"   优先级: {task.priority} | 创建时间: {task.timestamps['created_at']}\n"
                f"   {'-'*40}"
            )

async def start_service(cookie_file=None):
    """启动任务处理服务 (带状态监控)"""
    try:
        scheduler = TaskScheduler(cookie_file=Path(cookie_file) if cookie_file else None)
        print("🚀🚀 启动任务处理服务... (Ctrl+C 停止)")
        try:
            # 兼容 Python 3.10：不用 TaskGroup，改用 gather
            await asyncio.gather(
                scheduler.start(),
                scheduler.monitor_status()
            )
        except Exception as ex:
            # 更详细的错误处理
            print(f"⚠️ 服务异常: {str(ex)}")
            import traceback
            traceback.print_exc()
            scheduler.error_info = {
                'type': type(ex).__name__,
                'message': str(ex)
            }
    except Exception as e:
        print(f"❌❌ 服务启动失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    # 通过检查scheduler实例的error_info属性判断是否有错误
    return not hasattr(scheduler, 'error_info')
def parse_args():
    parser = argparse.ArgumentParser(
        description="YouTube视频自动化处理系统",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument('-v', '--version', action='version', 
                       version=f"YT Processor v{get_version()}")
    subparsers = parser.add_subparsers(dest='command', title='可用命令')

    # run 命令
    run_parser = subparsers.add_parser('run', help='启动处理服务')
    run_parser.add_argument('--cookie', dest='cookie_file', type=str, default=None,
                           help='指定 cookie 文件路径（优先级最高）')

    # create 命令增强
    create_parser = subparsers.add_parser(
        'create', 
        help='创建新任务\n示例: create "https://youtu.be/example" -p 3 --quality 720p'
    )
    create_parser.add_argument('url', help='YouTube视频URL')
    create_parser.add_argument('-p', '--priority', type=int, default=5,
                              choices=range(1, 10), metavar='[1-9]',
                              help='任务优先级 (默认: 5)')
    create_parser.add_argument('--quality', default='1080p',
                              choices=['480p', '720p', '1080p', '4K'],
                              help='视频质量要求')

    # list 命令增强
    list_parser = subparsers.add_parser('list', help='查询任务列表')
    list_parser.add_argument('-s', '--status', 
                            choices=['pending', 'processing', 'completed', 'failed'],
                            help='按状态过滤')
    list_parser.add_argument('-l', '--limit', type=int, default=10,
                            help='显示数量限制 (默认: 10)')

    # details 命令
    details_parser = subparsers.add_parser('details', help='查询前 n 个任务的所有阶段详情')
    details_parser.add_argument('n', type=int, help='要查询的任务数量')

    # delete 命令
    delete_parser = subparsers.add_parser('delete', help='删除指定任务')
    delete_parser.add_argument('task_id', help='任务ID')

    return parser.parse_args()

async def main():
    args = parse_args()
    
    try:
        if args.command == 'run':
            cookie_file = args.cookie_file if hasattr(args, 'cookie_file') else None
            success = await start_service(cookie_file=cookie_file)
            if not success:
                sys.exit(1)  # 非零退出码表示错误
        elif args.command == 'create':
            await TaskCLI.create_task(args.url, args.priority)
        elif args.command == 'list':
            await TaskCLI.list_tasks(args.status, args.limit)
        elif args.command == 'details':
            await TaskCLI.show_tasks_detail(args.n)
        elif args.command == 'delete':
            await TaskCLI.delete_task(args.task_id)
        else:
            print("""\n请使用以下有效命令：
  run       启动处理服务
  create    创建新任务 (需URL参数)
  list      查看任务列表
  details   查看前 n 个任务的所有阶段详情
  delete    删除指定任务 (需任务ID)
            """)
    except (ValueError, RuntimeError) as e:
        print(f"\n错误: {str(e)}")
    except KeyboardInterrupt:
        print("\n操作已取消")

if __name__ == "__main__":
    asyncio.run(main())