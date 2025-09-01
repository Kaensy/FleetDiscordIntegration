import discord
from discord.ext import commands
from discord import ui
from datetime import datetime, timedelta
from typing import Optional, List
import logging

logger = logging.getLogger(__name__)


class DaySelectView(ui.View):
    """Interactive button view for selecting time period"""

    def __init__(self, callback_func, driver_uuid=None):
        super().__init__(timeout=60)
        self.callback_func = callback_func
        self.driver_uuid = driver_uuid
        self.selected_days = None

    @ui.button(label="1 Day", style=discord.ButtonStyle.primary)
    async def one_day(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        await self.callback_func(interaction, 1, self.driver_uuid)
        self.stop()

    @ui.button(label="3 Days", style=discord.ButtonStyle.primary)
    async def three_days(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        await self.callback_func(interaction, 3, self.driver_uuid)
        self.stop()

    @ui.button(label="7 Days", style=discord.ButtonStyle.primary)
    async def seven_days(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        await self.callback_func(interaction, 7, self.driver_uuid)
        self.stop()

    @ui.button(label="14 Days", style=discord.ButtonStyle.primary)
    async def fourteen_days(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        await self.callback_func(interaction, 14, self.driver_uuid)
        self.stop()

    @ui.button(label="30 Days", style=discord.ButtonStyle.success)
    async def thirty_days(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        await self.callback_func(interaction, 30, self.driver_uuid)
        self.stop()

    @ui.button(label="ALL TIME", style=discord.ButtonStyle.danger)
    async def all_time(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        await self.callback_func(interaction, None, self.driver_uuid)
        self.stop()


class FleetCommands(commands.Cog):
    """Custom fleet management commands for DesiSquad"""

    def __init__(self, bot):
        self.bot = bot
        self.bolt_client = bot.bolt_client

    @commands.hybrid_command(name="fleet-stats", description="Get DesiSquad fleet statistics")
    async def fleet_stats(self, ctx):
        """Display fleet statistics"""
        try:
            if hasattr(ctx, 'defer'):
                await ctx.defer()
            else:
                async with ctx.typing():
                    pass

            # Get stats from database
            week_stats = self.bolt_client.db.get_fleet_stats(days=7)
            all_time_stats = self.bolt_client.db.get_fleet_stats()
            db_stats = self.bolt_client.db.get_database_stats()

            embed = discord.Embed(
                title="🚗 DesiSquad Fleet Information",
                color=0x00ff00,
                timestamp=datetime.now()
            )

            # 7-day stats
            embed.add_field(
                name="📊 Last 7 Days",
                value=(
                    f"**Trips Completed:** {week_stats['total_trips']}\n"
                    f"**Distance Traveled:** {week_stats['total_distance_km']} km"
                ),
                inline=True
            )

            # All-time stats
            embed.add_field(
                name="📈 All Time",
                value=(
                    f"**Trips Completed:** {all_time_stats['total_trips']}\n"
                    f"**Distance Traveled:** {all_time_stats['total_distance_km']} km"
                ),
                inline=True
            )

            # Database info
            embed.add_field(
                name="💾 Database",
                value=(
                    f"**Total Orders:** {db_stats['total_orders']:,}\n"
                    f"**Size:** {db_stats['database_size_mb']} MB"
                ),
                inline=True
            )

            embed.set_footer(text="Data from local database • Use !sync to update")

            if hasattr(ctx, 'followup'):
                await ctx.followup.send(embed=embed)
            else:
                await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Fleet stats command failed: {e}")
            error_msg = f"❌ Failed to fetch fleet statistics: {str(e)}"
            if hasattr(ctx, 'followup'):
                await ctx.followup.send(error_msg, ephemeral=True)
            else:
                await ctx.send(error_msg)

    @commands.hybrid_command(name="drivers", description="List all drivers")
    async def drivers_list(self, ctx):
        """Display list of all drivers"""
        try:
            if hasattr(ctx, 'defer'):
                await ctx.defer()
            else:
                async with ctx.typing():
                    pass

            drivers = self.bolt_client.db.get_all_drivers()

            if not drivers:
                msg = "No drivers found. Run !sync to update data."
                if hasattr(ctx, 'followup'):
                    await ctx.followup.send(msg)
                else:
                    await ctx.send(msg)
                return

            embed = discord.Embed(
                title="👥 Driver List",
                description="Use driver number with !driver-stats command",
                color=0x0099ff,
                timestamp=datetime.now()
            )

            driver_list = []
            for num, uuid, name in drivers:
                driver_list.append(f"**{num}.** {name}")

            embed.add_field(
                name="Drivers",
                value='\n'.join(driver_list) if driver_list else "No drivers",
                inline=False
            )

            embed.set_footer(text="Use: !driver-stats [number] [days]")

            if hasattr(ctx, 'followup'):
                await ctx.followup.send(embed=embed)
            else:
                await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Drivers list command failed: {e}")
            await ctx.send(f"❌ Failed to fetch drivers: {str(e)}")

    @commands.hybrid_command(name="driver-stats", description="Get driver statistics")
    async def driver_stats(self, ctx, driver_number: int, days: Optional[int] = None):
        """Display driver statistics with interactive day selection"""
        try:
            if hasattr(ctx, 'defer'):
                await ctx.defer()
            else:
                async with ctx.typing():
                    pass

            # Get driver UUID from number
            drivers = self.bolt_client.db.get_all_drivers()
            if driver_number < 1 or driver_number > len(drivers):
                await ctx.send("❌ Invalid driver number. Use !drivers to see the list.")
                return

            driver_uuid = drivers[driver_number - 1][1]
            driver_name = drivers[driver_number - 1][2]

            if days is None:
                # Show interactive buttons
                view = DaySelectView(self._show_driver_stats, driver_uuid)
                msg = await ctx.send(
                    f"📊 Select time period for **{driver_name}**'s statistics:",
                    view=view
                )
            else:
                # Show stats directly
                await self._show_driver_stats_direct(ctx, days, driver_uuid)

        except Exception as e:
            logger.error(f"Driver stats command failed: {e}")
            await ctx.send(f"❌ Failed to fetch driver stats: {str(e)}")

    async def _show_driver_stats_direct(self, ctx, days: Optional[int], driver_uuid: str,
                                        state_logs: Optional[List] = None):
        """Show driver stats directly without buttons"""
        if not state_logs:
            # Fetch state logs if not provided
            async with self.bolt_client:
                if days:
                    # Calculate proper date range for full days
                    now = datetime.now()
                    # Start at midnight X days ago
                    start_date = datetime(now.year, now.month, now.day) - timedelta(days=days - 1)
                    end_date = now  # Current moment
                else:
                    start_date = datetime(2024, 7, 28)  # Company start date
                    end_date = datetime.now()

                state_response = await self.bolt_client.get_fleet_state_logs(
                    start_date=start_date,
                    end_date=end_date,
                    limit=1000
                )
                state_logs = state_response.get('data', {}).get('state_logs', []) if state_response.get(
                    'code') == 0 else []

        stats = self.bolt_client.db.get_driver_stats_by_uuid(driver_uuid, days, state_logs)

        if not stats:
            await ctx.send("No data found for this period.")
            return

        period_text = f"{days} days ({stats['date_range']})" if days else stats['date_range']

        embed = discord.Embed(
            title=f"👤 {stats['driver_name']} - {period_text}",
            color=0xff9500,
            timestamp=datetime.now()
        )

        embed.add_field(
            name="📊 Orders & Earnings",
            value=(
                f"**Orders Completed:** {stats['orders_completed']}\n"
                f"**Gross Earnings:** {stats['gross_earnings']} RON\n"
                f"**Net Earnings:** {stats['net_earnings']} RON\n"
                f"**💵 Cash Collected:** {stats['cash_collected']} RON"
            ),
            inline=False
        )

        embed.add_field(
            name="📍 Distance & Time",
            value=(
                f"**Total Distance:** {stats['total_distance']} km\n"
                f"**Total Online Hours:** {stats['hours_worked']} hrs\n"
                f"**├─ On Rides:** {stats.get('ride_hours', stats['hours_worked'])} hrs\n"
                f"**└─ Waiting:** {stats.get('waiting_hours', 0)} hrs\n"
                f"**Avg Distance/Trip:** {stats['avg_distance']} km"
            ),
            inline=False
        )

        # Calculate earnings based on total online hours for realistic metrics
        earnings_per_online_hour = stats['gross_earnings'] / stats['hours_worked'] if stats['hours_worked'] > 0 else 0

        # Calculate earnings based on ride hours only (actual working time)
        ride_hours = stats.get('ride_hours', stats['hours_worked'])
        earnings_per_ride_hour = stats['gross_earnings'] / ride_hours if ride_hours > 0 else 0

        embed.add_field(
            name="💰 Performance Metrics",
            value=(
                f"**Earnings/Online Hour:** {round(earnings_per_online_hour, 2)} RON/hr\n"
                f"**Earnings/Ride Hour:** {round(earnings_per_ride_hour, 2)} RON/hr\n"
                f"**Earnings/KM:** {stats['earnings_per_km']} RON/km"
            ),
            inline=False
        )

        if hasattr(ctx, 'followup'):
            await ctx.followup.send(embed=embed)
        else:
            await ctx.send(embed=embed)

    async def _show_driver_stats(self, interaction: discord.Interaction, days: Optional[int], driver_uuid: str):
        """Callback for showing driver stats after button selection"""
        # Fetch state logs for accurate hours with proper date ranges
        async with self.bolt_client:
            if days:
                # Calculate proper date range for full days
                now = datetime.now()
                # Start at midnight X days ago
                start_date = datetime(now.year, now.month, now.day) - timedelta(days=days - 1)
                end_date = now  # Current moment
            else:
                start_date = datetime(2024, 7, 28)  # Company start date
                end_date = datetime.now()

            state_response = await self.bolt_client.get_fleet_state_logs(
                start_date=start_date,
                end_date=end_date,
                limit=1000
            )
            state_logs = state_response.get('data', {}).get('state_logs', []) if state_response.get('code') == 0 else []

        stats = self.bolt_client.db.get_driver_stats_by_uuid(driver_uuid, days, state_logs)

        if not stats:
            await interaction.followup.send("No data found for this period.")
            return

        period_text = f"{days} days ({stats['date_range']})" if days else stats['date_range']

        embed = discord.Embed(
            title=f"👤 {stats['driver_name']} - {period_text}",
            color=0xff9500,
            timestamp=datetime.now()
        )

        embed.add_field(
            name="📊 Orders & Earnings",
            value=(
                f"**Orders Completed:** {stats['orders_completed']}\n"
                f"**Gross Earnings:** {stats['gross_earnings']} RON\n"
                f"**Net Earnings:** {stats['net_earnings']} RON\n"
                f"**💵 Cash Collected:** {stats['cash_collected']} RON"
            ),
            inline=False
        )

        embed.add_field(
            name="📍 Distance & Time",
            value=(
                f"**Total Distance:** {stats['total_distance']} km\n"
                f"**Total Online Hours:** {stats['hours_worked']} hrs\n"
                f"**├─ On Rides:** {stats.get('ride_hours', stats['hours_worked'])} hrs\n"
                f"**└─ Waiting:** {stats.get('waiting_hours', 0)} hrs\n"
                f"**Avg Distance/Trip:** {stats['avg_distance']} km"
            ),
            inline=False
        )

        # Calculate earnings based on total online hours for realistic metrics
        earnings_per_online_hour = stats['gross_earnings'] / stats['hours_worked'] if stats['hours_worked'] > 0 else 0

        # Calculate earnings based on ride hours only (actual working time)
        ride_hours = stats.get('ride_hours', stats['hours_worked'])
        earnings_per_ride_hour = stats['gross_earnings'] / ride_hours if ride_hours > 0 else 0

        embed.add_field(
            name="💰 Performance Metrics",
            value=(
                f"**Earnings/Online Hour:** {round(earnings_per_online_hour, 2)} RON/hr\n"
                f"**Earnings/Ride Hour:** {round(earnings_per_ride_hour, 2)} RON/hr\n"
                f"**Earnings/KM:** {stats['earnings_per_km']} RON/km"
            ),
            inline=False
        )

        await interaction.followup.send(embed=embed)


    @commands.hybrid_command(name="company-earnings", description="Get company earnings")
    async def company_earnings(self, ctx, days: Optional[int] = None):
        """Display company earnings with interactive day selection"""
        try:
            if hasattr(ctx, 'defer'):
                await ctx.defer()
            else:
                async with ctx.typing():
                    pass

            if days is None:
                # Show interactive buttons
                view = DaySelectView(self._show_company_earnings)
                await ctx.send("📊 Select time period for company earnings:", view=view)
            else:
                # Show stats directly
                await self._show_company_earnings_direct(ctx, days)

        except Exception as e:
            logger.error(f"Company earnings command failed: {e}")
            await ctx.send(f"❌ Failed to fetch earnings: {str(e)}")

    async def _show_company_earnings(self, interaction: discord.Interaction, days: Optional[int], driver_uuid=None):
        """Callback for showing company earnings after button selection"""
        stats = self.bolt_client.db.get_company_earnings(days)

        period_text = f"{days} days ({stats['date_range']})" if days else stats['date_range']

        embed = discord.Embed(
            title=f"💰 DesiSquad Earnings - {period_text}",
            color=0x00d4aa,
            timestamp=datetime.now()
        )

        embed.add_field(
            name="💵 Earnings",
            value=(
                f"**Gross Earnings:** {stats['gross_earnings']} RON\n"
                f"**Net Earnings:** {stats['net_earnings']} RON"
            ),
            inline=True
        )

        embed.add_field(
            name="📊 Trips",
            value=(
                f"**Trips Completed:** {stats['trips_completed']}\n"
                f"**Total Distance:** {stats['total_distance']} km"
            ),
            inline=True
        )

        embed.add_field(
            name="📈 Metrics",
            value=(
                f"**Earnings/Trip:** {stats['earnings_per_trip']} RON\n"
                f"**Earnings/KM:** {stats['earnings_per_km']} RON"
            ),
            inline=False
        )

        await interaction.followup.send(embed=embed)

    async def _show_company_earnings_direct(self, ctx, days: Optional[int]):
        """Show company earnings directly without buttons"""
        stats = self.bolt_client.db.get_company_earnings(days)

        period_text = f"{days} days ({stats['date_range']})" if days else stats['date_range']

        embed = discord.Embed(
            title=f"💰 DesiSquad Earnings - {period_text}",
            color=0x00d4aa,
            timestamp=datetime.now()
        )

        embed.add_field(
            name="💵 Earnings",
            value=(
                f"**Gross Earnings:** {stats['gross_earnings']} RON\n"
                f"**Net Earnings:** {stats['net_earnings']} RON"
            ),
            inline=True
        )

        embed.add_field(
            name="📊 Trips",
            value=(
                f"**Trips Completed:** {stats['trips_completed']}\n"
                f"**Total Distance:** {stats['total_distance']} km"
            ),
            inline=True
        )

        embed.add_field(
            name="📈 Metrics",
            value=(
                f"**Earnings/Trip:** {stats['earnings_per_trip']} RON\n"
                f"**Earnings/KM:** {stats['earnings_per_km']} RON"
            ),
            inline=False
        )

        if hasattr(ctx, 'followup'):
            await ctx.followup.send(embed=embed)
        else:
            await ctx.send(embed=embed)

    @commands.command(name="sync")
    @commands.cooldown(1, 60, commands.BucketType.guild)
    async def sync_database(self, ctx, full: bool = False):
        """Sync orders from Bolt API to local database"""
        try:
            await ctx.send("🔄 Starting database sync...")

            async with self.bolt_client:
                result = await self.bolt_client.sync_database(full_sync=full)

            embed = discord.Embed(
                title="✅ Database Sync Complete",
                color=0x00ff00,
                timestamp=datetime.now()
            )

            embed.add_field(name="New Orders", value=result['new_orders'], inline=True)
            embed.add_field(name="Updated Orders", value=result['updated_orders'], inline=True)
            embed.add_field(name="Total Processed", value=result['total_processed'], inline=True)

            # Get current database stats
            db_stats = self.bolt_client.db.get_database_stats()
            embed.add_field(
                name="Database Status",
                value=f"**Total Orders:** {db_stats['total_orders']:,}\n**Size:** {db_stats['database_size_mb']} MB",
                inline=False
            )

            await ctx.send(embed=embed)

        except commands.CommandOnCooldown as e:
            await ctx.send(f"⏰ Sync is on cooldown. Try again in {e.retry_after:.0f} seconds.")
        except Exception as e:
            logger.error(f"Sync command failed: {e}")
            await ctx.send(f"❌ Sync failed: {str(e)}")

    @commands.command(name="debug-driver")
    @commands.has_permissions(administrator=True)
    async def debug_driver_calculation(self, ctx, driver_number: int):
        """Debug driver calculation methods"""
        try:
            drivers = self.bolt_client.db.get_all_drivers()
            if driver_number < 1 or driver_number > len(drivers):
                await ctx.send("❌ Invalid driver number.")
                return

            driver_uuid = drivers[driver_number - 1][1]
            driver_name = drivers[driver_number - 1][2]

            await ctx.send(f"🔧 Running debug analysis for **{driver_name}**...")

            embed = discord.Embed(
                title=f"🔧 Debug Analysis for {driver_name}",
                description="Run the debug script locally for detailed analysis",
                color=0xffa500
            )

            embed.add_field(
                name="Debug Steps",
                value=(
                    "1. Run `python test_active_hours.py`\n"
                    "2. Check console output for discrepancies\n"
                    "3. Compare with website's 'Active online time'\n"
                    "4. Should be very close to 2h 38min"
                ),
                inline=False
            )

            embed.add_field(
                name="Key Points",
                value=(
                    "• 'Active Hours' = time engaged with customers\n"
                    "• Calculated from order acceptance to completion\n"
                    "• Should match website's 'Active online time'\n"
                    "• State logs used when available (≤30 days)"
                ),
                inline=False
            )

            await ctx.send(embed=embed)

        except Exception as e:
            await ctx.send(f"❌ Debug failed: {str(e)}")


async def setup(bot):
    await bot.add_cog(FleetCommands(bot))