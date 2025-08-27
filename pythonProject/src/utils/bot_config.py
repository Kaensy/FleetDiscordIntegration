# src/utils/bot_config.py
import json
from pathlib import Path
from typing import Optional


class BotConfiguration:
    """Persistent bot configuration storage"""

    def __init__(self, config_file: str = "bot_config.json"):
        self.config_file = Path(config_file)
        self.config = self._load_config()

    def _load_config(self) -> dict:
        """Load configuration from file"""
        if self.config_file.exists():
            with open(self.config_file, 'r') as f:
                return json.load(f)
        return {}

    def _save_config(self):
        """Save configuration to file"""
        with open(self.config_file, 'w') as f:
            json.dump(self.config, f, indent=2)

    def get_report_channel(self) -> Optional[int]:
        """Get the report channel ID"""
        return self.config.get('report_channel_id')

    def set_report_channel(self, channel_id: int):
        """Set the report channel ID"""
        self.config['report_channel_id'] = channel_id
        self._save_config()

    def get_company_start_date(self) -> str:
        """Get company start date"""
        return self.config.get('company_start_date', '2024-07-28')

    def set_company_start_date(self, date: str):
        """Set company start date"""
        self.config['company_start_date'] = date
        self._save_config()