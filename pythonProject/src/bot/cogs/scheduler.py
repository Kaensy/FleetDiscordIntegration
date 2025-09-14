# src/bot/cogs/scheduler.py
from typing import List, Dict, Any

import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone, timedelta
import logging
import json
from pathlib import Path
import pytz

logger = logging.getLogger(__name__)


class ScheduledTasks(commands.Cog):
    """Scheduled tasks for periodic updates and reports"""

    def __init__(self, bot):
        self.bot = bot
        self.bolt_client = bot.bolt_client
        self.report_channel_id = None  # Main report channel for summary
        self.driver_channels = {}  # Will store driver_uuid -> channel_id mapping
        self.config_file = Path("data/driver_channels.json")
        self.settings_file = Path("data/scheduler_settings.json")

        # Ensure data directory exists
        self.config_file.parent.mkdir(exist_ok=True)

        # Load settings
        self.load_settings()
        self.load_driver_channels()

        # Start scheduled tasks
        self.sync_database.start()
        self.midnight_report.start()

        logger.info("Scheduler initialized with tasks")

    def load_settings(self):
        """Load scheduler settings including report channel"""
        if self.settings_file.exists():
            try:
                with open(self.settings_file, 'r') as f:
                    settings = json.load(f)
                    self.report_channel_id = settings.get('report_channel_id')
                    logger.info(f"Loaded settings: report_channel={self.report_channel_id}")
            except Exception as e:
                logger.error(f"Failed to load settings: {e}")

    def save_settings(self):
        """Save scheduler settings"""
        try:
            settings = {
                'report_channel_id': self.report_channel_id
            }
            with open(self.settings_file, 'w') as f:
                json.dump(settings, f, indent=2)
            logger.info("Saved scheduler settings")
        except Exception as e:
            logger.error(f"Failed to save settings: {e}")

    def cog_unload(self):
        """Cancel tasks when cog is unloaded"""
        self.sync_database.cancel()
        self.midnight_report.cancel()

    def load_driver_channels(self):
        """Load driver channel mappings from file"""
        if self.config_file.exists():
            try:
                with open(self.config_file, 'r') as f:
                    self.driver_channels = json.load(f)
                logger.info(f"Loaded {len(self.driver_channels)} driver channel mappings")
            except Exception as e:
                logger.error(f"Failed to load driver channels: {e}")
                self.driver_channels = {}

    def save_driver_channels(self):
        """Save driver channel mappings to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.driver_channels, f, indent=2)
            logger.info("Saved driver channel mappings")
        except Exception as e:
            logger.error(f"Failed to save driver channels: {e}")

    @tasks.loop(minutes=10)
    async def sync_database(self):
        """Sync database every 10 minutes"""
        try:
            logger.info("Starting scheduled database sync...")

            async with self.bolt_client:
                result = await self.bolt_client.sync_database(full_sync=False)

            if result['new_orders'] > 0:
                logger.info(f"Database sync completed: {result['new_orders']} new orders added")

                # Notify if significant new orders
                if result['new_orders'] > 10 and self.report_channel_id:
                    channel = self.bot.get_channel(self.report_channel_id)
                    if channel:
                        await channel.send(f"📊 Database updated: {result['new_orders']} new orders synced")

        except Exception as e:
            logger.error(f"Database sync failed: {e}", exc_info=True)

    @tasks.loop(time=[time(hour=21, minute=0), time(hour=22, minute=0)])  # 21:00 and 22:00 UTC to handle DST
    async def midnight_report(self):
        """Send daily reports at midnight Romania time"""
        try:
            # Get current time in Romania
            romania_tz = pytz.timezone('Europe/Bucharest')
            now_romania = datetime.now(romania_tz)

            # Check if it's actually around midnight in Romania (23:55 - 00:05)
            if not (now_romania.hour == 23 and now_romania.minute >= 55) and not (
                    now_romania.hour == 0 and now_romania.minute <= 5):
                logger.info(f"Skipping report - Romania time is {now_romania.strftime('%H:%M')}, not midnight")
                return

            logger.info(f"Starting midnight report generation at {now_romania.strftime('%Y-%m-%d %H:%M:%S %Z')}")

            # Check if report channel is configured
            if not self.report_channel_id:
                logger.error("Report channel not configured! Use !set-report-channel to configure")
                return

            # Get yesterday's date (since we're running at midnight)
            yesterday = now_romania - timedelta(days=1)
            report_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

            # Fetch state logs for accurate hours calculation
            async with self.bolt_client:
                start_of_day = report_date
                end_of_day = start_of_day + timedelta(days=1)

                state_response = await self.bolt_client.get_fleet_state_logs(
                    start_date=start_of_day,
                    end_date=end_of_day,
                    limit=1000
                )
                state_logs = state_response.get('data', {}).get('state_logs', []) if state_response.get(
                    'code') == 0 else []

            # Get enhanced daily stats using the same logic as driver-stats
            driver_stats = self.get_enhanced_daily_stats(report_date, state_logs)

            if not driver_stats:
                logger.info("No driver activity for the report date")
                channel = self.bot.get_channel(self.report_channel_id)
                if channel:
                    await channel.send(f"📊 No driver activity on {report_date.strftime('%Y-%m-%d')}")
                return

            # Send main summary report to the main channel
            await self.send_summary_report(driver_stats, report_date)

            # Send individual reports to each driver's channel
            await self.send_individual_reports(driver_stats, report_date, state_logs)

            logger.info("Midnight reports sent successfully")

        except Exception as e:
            logger.error(f"Midnight report failed: {e}", exc_info=True)
            # Try to notify about the error
            if self.report_channel_id:
                try:
                    channel = self.bot.get_channel(self.report_channel_id)
                    if channel:
                        await channel.send(f"❌ Midnight report failed: {str(e)[:200]}")
                except:
                    pass

    def get_enhanced_daily_stats(self, report_date: datetime, state_logs: List[Dict]) -> List[Dict[str, Any]]:
        """Get daily stats with proper hours calculation using the same logic as driver-stats"""

        # Get all drivers who worked that day
        basic_stats = self.bolt_client.db.get_driver_daily_stats(report_date)

        if not basic_stats:
            return []

        enhanced_stats = []

        # Calculate date range for the day
        start_date = report_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1)

        for driver in basic_stats:
            driver_uuid = driver['driver_uuid']

            # Use the same comprehensive calculation as driver-stats
            detailed_stats = self.bolt_client.db.get_driver_stats_by_date_range(
                driver_uuid,
                start_date,
                end_date,
                state_logs
            )

            if detailed_stats:
                # Build enhanced stats combining both calculations
                enhanced_driver = {
                    'driver_uuid': driver_uuid,
                    'driver_name': driver['driver_name'],
                    'orders_completed': driver['orders_completed'],
                    'gross_earnings': driver['gross_earnings'],
                    'net_earnings': driver['net_earnings'],
                    'kms_traveled': driver['kms_traveled'],
                    'cash_collected': driver['cash_collected'],
                    # Use the properly calculated hours from driver-stats logic
                    'hours_worked': detailed_stats['hours_worked'],  # Active hours
                    'total_online_hours': detailed_stats['total_online_hours'],  # Total online
                    'waiting_hours': detailed_stats['waiting_hours'],  # Waiting time
                    'earnings_per_hour': round(driver['gross_earnings'] / detailed_stats['total_online_hours'], 2) if
                    detailed_stats['total_online_hours'] > 0 else 0
                }
                enhanced_stats.append(enhanced_driver)
            else:
                # Fallback to basic stats if detailed calculation fails
                enhanced_stats.append(driver)

        return enhanced_stats

    async def send_summary_report(self, driver_stats, report_date):
        """Send the summary report to the main channel with corrected hours"""
        if not self.report_channel_id:
            logger.warning("Main report channel ID not configured")
            return

        channel = self.bot.get_channel(self.report_channel_id)
        if not channel:
            logger.error(f"Could not find main report channel {self.report_channel_id}")
            return

        # Calculate totals using the enhanced stats
        total_orders = sum(d['orders_completed'] for d in driver_stats)
        total_gross = sum(d['gross_earnings'] for d in driver_stats)
        total_net = sum(d['net_earnings'] for d in driver_stats)
        total_cash = sum(d['cash_collected'] for d in driver_stats)
        total_kms = sum(d['kms_traveled'] for d in driver_stats)

        # Use total_online_hours if available, otherwise hours_worked
        total_hours = sum(d.get('total_online_hours', d.get('hours_worked', 0)) for d in driver_stats)
        total_active_hours = sum(d.get('hours_worked', 0) for d in driver_stats)
        total_waiting_hours = sum(d.get('waiting_hours', 0) for d in driver_stats)

        # Create summary embed
        embed = discord.Embed(
            title=f"📊 Daily Fleet Performance Report",
            description=f"Date: {report_date.strftime('%A, %B %d, %Y')}",
            color=0x00d4aa,
            timestamp=datetime.now(timezone.utc)
        )

        # Fleet totals with enhanced hours display
        hours_display = f"**Total Hours:** {total_hours:.1f} hrs"
        if total_active_hours > 0 and total_waiting_hours > 0:
            hours_display = (
                f"**Total Online:** {total_hours:.1f} hrs\n"
                f"├─ **Active:** {total_active_hours:.1f} hrs\n"
                f"└─ **Waiting:** {total_waiting_hours:.1f} hrs"
            )

        embed.add_field(
            name="📈 Fleet Totals",
            value=(
                f"**Total Orders:** {total_orders}\n"
                f"**Gross Earnings:** {total_gross:.2f} RON\n"
                f"**Net Earnings:** {total_net:.2f} RON\n"
                f"**Cash Collected:** {total_cash:.2f} RON\n"
                f"**Total Distance:** {total_kms:.1f} km\n"
                f"{hours_display}"
            ),
            inline=False
        )

        # Fleet averages
        if total_orders > 0 and total_hours > 0:
            embed.add_field(
                name="📊 Fleet Averages",
                value=(
                    f"**Avg per Order:** {total_gross / total_orders:.2f} RON\n"
                    f"**Avg per Hour (Total):** {total_gross / total_hours:.2f} RON/hr\n"
                    f"**Avg per Hour (Active):** {total_gross / total_active_hours:.2f} RON/hr\n" if total_active_hours > 0 else "" +
                                                                                                                                 f"**Avg per KM:** {total_gross / total_kms:.2f} RON/km" if total_kms > 0 else f"**Avg per Order:** {total_gross / total_orders:.2f} RON"
                ),
                inline=False
            )

        # Driver summary
        driver_summary = []
        for driver in sorted(driver_stats, key=lambda x: x['gross_earnings'], reverse=True)[:5]:
            driver_summary.append(
                f"• **{driver['driver_name']}**: {driver['orders_completed']} orders, "
                f"{driver['gross_earnings']:.2f} RON"
            )

        if driver_summary:
            embed.add_field(
                name="🏆 Top Performers",
                value="\n".join(driver_summary),
                inline=False
            )

        embed.set_footer(text="DesiSquad Fleet Management")

        await channel.send(embed=embed)

    async def send_individual_reports(self, driver_stats, report_date, state_logs):
        """Send individual reports to each driver's channel with enhanced stats"""
        for driver in driver_stats:
            try:
                driver_uuid = driver['driver_uuid']

                # Skip if no channel configured for this driver
                if driver_uuid not in self.driver_channels:
                    logger.debug(f"No channel configured for driver {driver['driver_name']}")
                    continue

                channel_id = self.driver_channels[driver_uuid]
                channel = self.bot.get_channel(channel_id)

                if not channel:
                    logger.warning(f"Could not find channel {channel_id} for driver {driver['driver_name']}")
                    continue

                # Create personalized embed with enhanced hours display
                embed = discord.Embed(
                    title=f"📊 Daily Performance Report",
                    description=f"Driver: **{driver['driver_name']}**\nDate: {report_date.strftime('%A, %B %d, %Y')}",
                    color=0x0099ff,
                    timestamp=datetime.now(timezone.utc)
                )

                # Format hours display
                if 'total_online_hours' in driver:
                    # We have enhanced stats
                    active_h = int(driver['hours_worked'])
                    active_m = int((driver['hours_worked'] - active_h) * 60)

                    total_h = int(driver['total_online_hours'])
                    total_m = int((driver['total_online_hours'] - total_h) * 60)

                    waiting_h = int(driver['waiting_hours'])
                    waiting_m = int((driver['waiting_hours'] - waiting_h) * 60)

                    hours_display = (
                        f"**Total Online:** {total_h}h {total_m}m\n"
                        f"├─ **Active:** {active_h}h {active_m}m\n"
                        f"└─ **Waiting:** {waiting_h}h {waiting_m}m"
                    )
                else:
                    # Basic stats only
                    hours_display = f"**Hours Worked:** {driver['hours_worked']} hrs"

                # Performance metrics
                embed.add_field(
                    name="🚗 Activity",
                    value=(
                        f"**Orders Completed:** {driver['orders_completed']}\n"
                        f"{hours_display}\n"
                        f"**Distance Traveled:** {driver['kms_traveled']} km"
                    ),
                    inline=False
                )

                # Earnings
                embed.add_field(
                    name="💰 Earnings",
                    value=(
                        f"**Gross:** {driver['gross_earnings']} RON\n"
                        f"**Net:** {driver['net_earnings']} RON\n"
                        f"**💵 Cash:** {driver['cash_collected']} RON"
                    ),
                    inline=True
                )

                # Efficiency metrics
                if driver.get('total_online_hours', driver.get('hours_worked', 0)) > 0 and driver[
                    'orders_completed'] > 0:
                    hours_for_calc = driver.get('total_online_hours', driver['hours_worked'])

                    # Calculate both total and active earnings per hour if we have the data
                    if 'hours_worked' in driver and 'total_online_hours' in driver:
                        earnings_text = (
                            f"**Per Hour (Total):** {driver['gross_earnings'] / driver['total_online_hours']:.2f} RON/hr\n"
                            f"**Per Hour (Active):** {driver['gross_earnings'] / driver['hours_worked']:.2f} RON/hr\n" if
                            driver['hours_worked'] > 0 else ""
                        )
                    else:
                        earnings_text = f"**Per Hour:** {driver['earnings_per_hour']} RON/hr\n"

                    embed.add_field(
                        name="📈 Efficiency",
                        value=(
                            f"{earnings_text}"
                            f"**Per Order:** {driver['gross_earnings'] / driver['orders_completed']:.2f} RON\n"
                            f"**Per KM:** {driver['gross_earnings'] / driver['kms_traveled']:.2f} RON/km" if driver[
                                                                                                                 'kms_traveled'] > 0 else f"{earnings_text}**Per Order:** {driver['gross_earnings'] / driver['orders_completed']:.2f} RON"
                        ),
                        inline=True
                    )

                # Cash collection warning
                if driver['cash_collected'] > 0:
                    embed.add_field(
                        name="⚠️ Cash Collection Required",
                        value=f"Please remit **{driver['cash_collected']} RON** in cash",
                        inline=False
                    )
                    embed.color = 0xff9500  # Orange for cash collection

                embed.set_footer(text="DesiSquad Fleet Management • Keep up the great work!")

                await channel.send(embed=embed)
                logger.info(f"Sent report to {driver['driver_name']}")

            except Exception as e:
                logger.error(f"Failed to send report to driver {driver.get('driver_name', 'Unknown')}: {e}")

    @sync_database.before_loop
    async def before_sync_database(self):
        """Wait for bot to be ready before starting sync"""
        await self.bot.wait_until_ready()

        # Do initial sync on startup
        logger.info("Performing initial database sync...")
        try:
            async with self.bolt_client:
                result = await self.bolt_client.sync_database(full_sync=False)
            logger.info(f"Initial sync complete: {result['new_orders']} new orders")
        except Exception as e:
            logger.error(f"Initial sync failed: {e}")

    @midnight_report.before_loop
    async def before_midnight_report(self):
        """Wait for bot to be ready before starting midnight reports"""
        await self.bot.wait_until_ready()
        logger.info("Midnight report task is ready")

    # Command to set main report channel
    @commands.command(name="set-report-channel")
    @commands.has_permissions(administrator=True)
    async def set_report_channel(self, ctx, channel: discord.TextChannel = None):
        """Set the channel for main fleet reports"""
        channel = channel or ctx.channel
        self.report_channel_id = channel.id
        self.save_settings()  # Persist the setting

        await ctx.send(f"✅ Main report channel set to {channel.mention}")
        logger.info(f"Main report channel set to {channel.id} by {ctx.author}")

    # Command to set driver channel
    @commands.command(name="set-driver-channel")
    @commands.has_permissions(administrator=True)
    async def set_driver_channel(self, ctx, driver_number: int, channel: discord.TextChannel = None):
        """Set the channel for a specific driver's reports"""
        channel = channel or ctx.channel

        # Get driver UUID from number
        drivers = self.bolt_client.db.get_all_drivers()
        if driver_number < 1 or driver_number > len(drivers):
            await ctx.send("❌ Invalid driver number. Use !drivers to see the list.")
            return

        driver_uuid = drivers[driver_number - 1][1]
        driver_name = drivers[driver_number - 1][2]

        # Save mapping
        self.driver_channels[driver_uuid] = channel.id
        self.save_driver_channels()

        await ctx.send(f"✅ Reports for **{driver_name}** will be sent to {channel.mention}")
        logger.info(f"Driver channel set: {driver_name} -> {channel.id}")

    # Command to test midnight report
    @commands.command(name="test-midnight-report")
    @commands.has_permissions(administrator=True)
    async def test_midnight_report(self, ctx):
        """Test the midnight report functionality"""
        try:
            await ctx.send("🔄 Generating test midnight report...")

            # Check if report channel is set
            if not self.report_channel_id:
                await ctx.send("❌ Report channel not set! Use `!set-report-channel` first")
                return

            # Get yesterday's data for test
            romania_tz = pytz.timezone('Europe/Bucharest')
            now_romania = datetime.now(romania_tz)
            yesterday = now_romania - timedelta(days=1)
            report_date = yesterday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)

            # Fetch state logs
            async with self.bolt_client:
                start_of_day = report_date
                end_of_day = start_of_day + timedelta(days=1)

                state_response = await self.bolt_client.get_fleet_state_logs(
                    start_date=start_of_day,
                    end_date=end_of_day,
                    limit=1000
                )
                state_logs = state_response.get('data', {}).get('state_logs', []) if state_response.get(
                    'code') == 0 else []

            # Get daily stats
            driver_stats = self.get_enhanced_daily_stats(report_date, state_logs)

            if not driver_stats:
                await ctx.send(f"📊 No driver activity on {report_date.strftime('%Y-%m-%d')}")
                return

            # Send reports
            await self.send_summary_report(driver_stats, report_date)
            await self.send_individual_reports(driver_stats, report_date, state_logs)

            await ctx.send("✅ Test report generation complete!")

        except Exception as e:
            await ctx.send(f"❌ Test report failed: {str(e)[:500]}")
            logger.error(f"Test report failed: {e}", exc_info=True)

    # Command to force sync
    @commands.command(name="force-sync")
    @commands.has_permissions(administrator=True)
    async def force_sync(self, ctx):
        """Force an immediate database sync"""
        try:
            await ctx.send("🔄 Starting forced sync...")

            async with self.bolt_client:
                result = await self.bolt_client.sync_database(full_sync=False)

            await ctx.send(f"✅ Sync complete: {result['new_orders']} new orders, {result['updated_orders']} updated")

        except Exception as e:
            logger.error(f"Force sync failed: {e}")
            await ctx.send(f"❌ Sync failed: {str(e)}")

    # Command to check scheduler status
    @commands.command(name="scheduler-status")
    @commands.has_permissions(administrator=True)
    async def scheduler_status(self, ctx):
        """Check the status of scheduled tasks"""
        embed = discord.Embed(
            title="⏰ Scheduler Status",
            color=0x00ff00,
            timestamp=datetime.now(timezone.utc)
        )

        # Get Romania time
        romania_tz = pytz.timezone('Europe/Bucharest')
        now_romania = datetime.now(romania_tz)

        embed.add_field(
            name="Current Time",
            value=f"Romania: {now_romania.strftime('%Y-%m-%d %H:%M:%S %Z')}",
            inline=False
        )

        # Database sync status
        sync_status = "✅ Running" if self.sync_database.is_running() else "❌ Stopped"
        next_sync = self.sync_database.next_iteration
        embed.add_field(
            name="Database Sync",
            value=(
                f"**Status:** {sync_status}\n"
                f"**Interval:** Every 10 minutes\n"
                f"**Next Run:** {next_sync.strftime('%H:%M:%S') if next_sync else 'N/A'}"
            ),
            inline=False
        )

        # Midnight report status
        report_status = "✅ Running" if self.midnight_report.is_running() else "❌ Stopped"
        next_report = self.midnight_report.next_iteration
        embed.add_field(
            name="Midnight Report",
            value=(
                f"**Status:** {report_status}\n"
                f"**Schedule:** Daily at 00:00 Romania time\n"
                f"**Next Run:** {next_report.strftime('%Y-%m-%d %H:%M:%S UTC') if next_report else 'N/A'}"
            ),
            inline=False
        )

        # Channel configuration
        main_channel = self.bot.get_channel(self.report_channel_id) if self.report_channel_id else None
        embed.add_field(
            name="Channel Configuration",
            value=(
                f"**Main Report Channel:** {main_channel.mention if main_channel else '❌ Not set'}\n"
                f"**Driver Channels Configured:** {len(self.driver_channels)}"
            ),
            inline=False
        )

        await ctx.send(embed=embed)


async def setup(bot):
    await bot.add_cog(ScheduledTasks(bot))