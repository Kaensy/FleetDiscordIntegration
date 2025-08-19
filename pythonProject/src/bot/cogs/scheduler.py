import discord
from discord.ext import commands, tasks
from datetime import datetime, time, timezone
import logging

logger = logging.getLogger(__name__)


class ScheduledTasks(commands.Cog):
    """Scheduled tasks for periodic updates and reports"""

    def __init__(self, bot):
        self.bot = bot
        self.bolt_client = bot.bolt_client
        self.report_channel_id = None  # Set this to your desired channel ID

        # Start scheduled tasks
        self.daily_report.start()
        self.hourly_update.start()

    def cog_unload(self):
        """Cancel tasks when cog is unloaded"""
        self.daily_report.cancel()
        self.hourly_update.cancel()

    @tasks.loop(time=time(hour=9, minute=0, tzinfo=timezone.utc))
    async def daily_report(self):
        """Send daily fleet report at 9 AM UTC"""
        try:
            if not self.report_channel_id:
                logger.warning("Report channel ID not configured")
                return

            channel = self.bot.get_channel(self.report_channel_id)
            if not channel:
                logger.error(f"Could not find channel with ID {self.report_channel_id}")
                return

            async with self.bolt_client:
                # Get yesterday's data
                fleet_stats = await self.bolt_client.get_fleet_statistics()
                earnings_data = await self.bolt_client.get_earnings_data()

            embed = discord.Embed(
                title="📊 Daily Fleet Report",
                description="Yesterday's fleet performance summary",
                color=0x0099ff,
                timestamp=datetime.now(timezone.utc)
            )

            # Key metrics
            embed.add_field(
                name="🚗 Total Trips",
                value=str(fleet_stats.get('total_trips', 'N/A')),
                inline=True
            )
            embed.add_field(
                name="💰 Total Earnings",
                value=f"€{earnings_data.get('gross_earnings', 0):.2f}",
                inline=True
            )
            embed.add_field(
                name="⭐ Average Rating",
                value=f"{fleet_stats.get('average_rating', 0):.2f}",
                inline=True
            )

            # Performance indicators
            if 'performance_indicators' in fleet_stats:
                indicators = fleet_stats['performance_indicators']
                performance_text = []
                for indicator, value in indicators.items():
                    performance_text.append(f"• {indicator.replace('_', ' ').title()}: {value}")

                if performance_text:
                    embed.add_field(
                        name="📈 Performance Indicators",
                        value='\n'.join(performance_text[:5]),
                        inline=False
                    )

            embed.set_footer(text="Daily automated report • Bolt Fleet API")

            await channel.send(embed=embed)
            logger.info("Daily report sent successfully")

        except Exception as e:
            logger.error(f"Daily report failed: {e}")

    @tasks.loop(hours=1)
    async def hourly_update(self):
        """Hourly status check and alerts"""
        try:
            async with self.bolt_client:
                fleet_info = await self.bolt_client.get_fleet_info()

            # Check for alerts (customize based on your needs)
            alerts = []

            # Example alert conditions
            if fleet_info.get('active_drivers', 0) < 5:
                alerts.append("⚠️ Low driver availability: Less than 5 active drivers")

            if fleet_info.get('status') != 'active':
                alerts.append(f"🚨 Fleet status alert: {fleet_info.get('status')}")

            # Send alerts if any
            if alerts and self.report_channel_id:
                channel = self.bot.get_channel(self.report_channel_id)
                if channel:
                    alert_embed = discord.Embed(
                        title="⚠️ Fleet Alerts",
                        description='\n'.join(alerts),
                        color=0xff9500,
                        timestamp=datetime.now(timezone.utc)
                    )
                    await channel.send(embed=alert_embed)
                    logger.info(f"Sent {len(alerts)} fleet alerts")

        except Exception as e:
            logger.error(f"Hourly update failed: {e}")

    @daily_report.before_loop
    async def before_daily_report(self):
        """Wait for bot to be ready before starting daily reports"""
        await self.bot.wait_until_ready()

    @hourly_update.before_loop
    async def before_hourly_update(self):
        """Wait for bot to be ready before starting hourly updates"""
        await self.bot.wait_until_ready()

    @commands.command(name="set-report-channel")
    @commands.has_permissions(administrator=True)
    async def set_report_channel(self, ctx, channel: discord.TextChannel = None):
        """Set the channel for automated reports"""
        channel = channel or ctx.channel
        self.report_channel_id = channel.id

        await ctx.send(f"✅ Report channel set to {channel.mention}")
        logger.info(f"Report channel set to {channel.id} by {ctx.author}")

    @commands.command(name="test-report")
    @commands.has_permissions(administrator=True)
    async def test_report(self, ctx):
        """Test the daily report functionality"""
        try:
            await ctx.send("🔄 Generating test report...")

            # Temporarily set report channel to current channel
            old_channel_id = self.report_channel_id
            self.report_channel_id = ctx.channel.id

            # Run daily report
            await self.daily_report()

            # Restore original channel
            self.report_channel_id = old_channel_id

        except Exception as e:
            await ctx.send(f"❌ Test report failed: {str(e)}")


async def setup(bot):
    await bot.add_cog(ScheduledTasks(bot))