# Save this as src/bot/cogs/scheduler.py (REPLACE the existing one)

import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone, timedelta
import logging

logger = logging.getLogger(__name__)


class ScheduledTasks(commands.Cog):
    """Scheduled tasks for periodic updates and reports"""

    def __init__(self, bot):
        self.bot = bot
        self.bolt_client = bot.bolt_client
        self.report_channel_id = None  # Set this to your desired channel ID

        # Start scheduled tasks
        self.sync_database.start()
        self.midnight_report.start()

    def cog_unload(self):
        """Cancel tasks when cog is unloaded"""
        self.sync_database.cancel()
        self.midnight_report.cancel()

    @tasks.loop(minutes=10)
    async def sync_database(self):
        """Sync database every 10 minutes"""
        try:
            async with self.bolt_client:
                result = await self.bolt_client.sync_database(full_sync=False)

            if result['new_orders'] > 0:
                logger.info(f"Database sync: {result['new_orders']} new orders added")

                # Notify if significant new orders
                if result['new_orders'] > 10 and self.report_channel_id:
                    channel = self.bot.get_channel(self.report_channel_id)
                    if channel:
                        await channel.send(f"📊 Database updated: {result['new_orders']} new orders synced")

        except Exception as e:
            logger.error(f"Database sync failed: {e}")

    @tasks.loop(time=time(hour=22, minute=0, tzinfo=timezone.utc))  # Midnight Romania time (UTC+2)
    async def midnight_report(self):
        """Send daily driver report at midnight (Romania time)"""
        try:
            if not self.report_channel_id:
                logger.warning("Report channel ID not configured")
                return

            channel = self.bot.get_channel(self.report_channel_id)
            if not channel:
                logger.error(f"Could not find channel with ID {self.report_channel_id}")
                return

            # Get today's data (yesterday since it's run at midnight)
            today = datetime.now()
            driver_stats = self.bolt_client.db.get_driver_daily_stats(today)

            if not driver_stats:
                await channel.send("📊 No driver activity today.")
                return

            # Create main embed
            embed = discord.Embed(
                title=f"📊 Daily Driver Report - {today.strftime('%Y-%m-%d')}",
                description="Performance summary for all drivers",
                color=0x0099ff,
                timestamp=datetime.now(timezone.utc)
            )

            # Add totals
            total_orders = sum(d['orders_completed'] for d in driver_stats)
            total_gross = sum(d['gross_earnings'] for d in driver_stats)
            total_net = sum(d['net_earnings'] for d in driver_stats)
            total_cash = sum(d['cash_collected'] for d in driver_stats)
            total_kms = sum(d['kms_traveled'] for d in driver_stats)

            embed.add_field(
                name="📈 Daily Totals",
                value=(
                    f"**Total Orders:** {total_orders}\n"
                    f"**Gross Earnings:** {total_gross:.2f} RON\n"
                    f"**Net Earnings:** {total_net:.2f} RON\n"
                    f"**Cash Collected:** {total_cash:.2f} RON\n"
                    f"**Distance:** {total_kms:.1f} km"
                ),
                inline=False
            )

            # Send main embed
            await channel.send(embed=embed)

            # Send individual driver reports
            for driver in driver_stats:
                driver_embed = discord.Embed(
                    title=f"👤 {driver['driver_name']}",
                    color=0x00ff00,
                    timestamp=datetime.now(timezone.utc)
                )

                driver_embed.add_field(
                    name="📊 Performance",
                    value=(
                        f"**Orders Completed:** {driver['orders_completed']}\n"
                        f"**Hours Worked:** {driver['hours_worked']} hrs\n"
                        f"**KMs Traveled:** {driver['kms_traveled']} km"
                    ),
                    inline=True
                )

                driver_embed.add_field(
                    name="💰 Earnings",
                    value=(
                        f"**Gross:** {driver['gross_earnings']} RON\n"
                        f"**Net:** {driver['net_earnings']} RON\n"
                        f"**💵 CASH:** {driver['cash_collected']} RON"
                    ),
                    inline=True
                )

                driver_embed.add_field(
                    name="📈 Metrics",
                    value=(
                        f"**Earnings/Hr:** {driver['earnings_per_hour']} RON/hr\n"
                        f"**Avg/Order:** {driver['gross_earnings'] / driver['orders_completed']:.2f} RON"
                    ),
                    inline=False
                )

                # Highlight cash collection
                if driver['cash_collected'] > 0:
                    driver_embed.set_footer(text=f"⚠️ Cash to collect: {driver['cash_collected']} RON")

                await channel.send(embed=driver_embed)

            logger.info("Midnight report sent successfully")

        except Exception as e:
            logger.error(f"Midnight report failed: {e}")

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

    @commands.command(name="set-report-channel")
    @commands.has_permissions(administrator=True)
    async def set_report_channel(self, ctx, channel: discord.TextChannel = None):
        """Set the channel for automated reports"""
        channel = channel or ctx.channel
        self.report_channel_id = channel.id

        await ctx.send(f"✅ Report channel set to {channel.mention}")
        logger.info(f"Report channel set to {channel.id} by {ctx.author}")

    @commands.command(name="test-midnight-report")
    @commands.has_permissions(administrator=True)
    async def test_midnight_report(self, ctx):
        """Test the midnight report functionality"""
        try:
            await ctx.send("🔄 Generating test midnight report...")

            # Temporarily set report channel to current channel
            old_channel_id = self.report_channel_id
            self.report_channel_id = ctx.channel.id

            # Run midnight report
            await self.midnight_report()

            # Restore original channel
            self.report_channel_id = old_channel_id

        except Exception as e:
            await ctx.send(f"❌ Test report failed: {str(e)}")

    @commands.command(name="force-sync")
    @commands.has_permissions(administrator=True)
    async def force_sync(self, ctx):
        """Force an immediate database sync"""
        try:
            await ctx.send("🔄 Starting forced sync...")

            async with self.bolt_client:
                result = await self.bolt_client.sync_database(full_sync=False)

            await ctx.send(f"✅ Sync complete: {result['new_orders']} new orders")

        except Exception as e:
            logger.error(f"Force sync failed: {e}")
            await ctx.send(f"❌ Sync failed: {str(e)}")


async def setup(bot):
    await bot.add_cog(ScheduledTasks(bot))