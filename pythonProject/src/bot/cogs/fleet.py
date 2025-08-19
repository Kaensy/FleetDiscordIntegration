import discord
from discord.ext import commands
from datetime import datetime, timedelta
from typing import Optional
import logging

logger = logging.getLogger(__name__)


class FleetCommands(commands.Cog):
    """Fleet management commands for Discord bot"""

    def __init__(self, bot):
        self.bot = bot
        self.bolt_client = bot.bolt_client

    @commands.hybrid_command(name="fleet-info", description="Get general fleet information")
    async def fleet_info(self, ctx):
        """Display general fleet information"""
        try:
            await ctx.defer()

            async with self.bolt_client:
                fleet_info = await self.bolt_client.get_fleet_info()

            embed = discord.Embed(
                title="🚗 Fleet Information",
                color=0x00ff00,
                timestamp=datetime.now()
            )

            embed.add_field(
                name="Fleet Name",
                value=fleet_info.get('name', 'N/A'),
                inline=True
            )
            embed.add_field(
                name="Total Vehicles",
                value=fleet_info.get('vehicle_count', 'N/A'),
                inline=True
            )
            embed.add_field(
                name="Active Drivers",
                value=fleet_info.get('active_drivers', 'N/A'),
                inline=True
            )

            if 'status' in fleet_info:
                status_emoji = "🟢" if fleet_info['status'] == 'active' else "🔴"
                embed.add_field(
                    name="Fleet Status",
                    value=f"{status_emoji} {fleet_info['status'].title()}",
                    inline=True
                )

            embed.set_footer(text="Data from Bolt Fleet API")

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Fleet info command failed: {e}")
            await ctx.followup.send(f"❌ Failed to fetch fleet information: {str(e)}", ephemeral=True)

    @commands.hybrid_command(name="recent-trips", description="Get recent trip data")
    @discord.app_commands.describe(days="Number of days to look back (default: 7)")
    async def recent_trips(self, ctx, days: Optional[int] = 7):
        """Display recent trip information"""
        try:
            await ctx.defer()

            if days < 1 or days > 30:
                await ctx.followup.send("❌ Days must be between 1 and 30", ephemeral=True)
                return

            start_date = datetime.now() - timedelta(days=days)

            async with self.bolt_client:
                trips = await self.bolt_client.get_trip_data(start_date=start_date, limit=50)

            if not trips:
                await ctx.followup.send(f"No trips found in the last {days} days.")
                return

            embed = discord.Embed(
                title=f"📊 Recent Trips ({days} days)",
                description=f"Found {len(trips)} trips",
                color=0x0099ff,
                timestamp=datetime.now()
            )

            # Calculate summary statistics
            total_distance = sum(trip.get('distance_km', 0) for trip in trips)
            total_earnings = sum(trip.get('earnings_eur', 0) for trip in trips)
            avg_rating = sum(trip.get('rating', 0) for trip in trips if trip.get('rating', 0) > 0) / len(
                [t for t in trips if t.get('rating', 0) > 0]) if any(trip.get('rating', 0) > 0 for trip in trips) else 0

            embed.add_field(name="📍 Total Distance", value=f"{total_distance:.1f} km", inline=True)
            embed.add_field(name="💰 Total Earnings", value=f"€{total_earnings:.2f}", inline=True)
            embed.add_field(name="⭐ Avg Rating", value=f"{avg_rating:.2f}" if avg_rating > 0 else "N/A", inline=True)

            # Show recent trips
            trip_list = []
            for trip in trips[:5]:  # Show last 5 trips
                trip_time = trip.get('completed_at', 'Unknown')
                trip_earnings = trip.get('earnings_eur', 0)
                trip_distance = trip.get('distance_km', 0)
                trip_list.append(f"• **{trip_time}** - €{trip_earnings:.2f} ({trip_distance:.1f}km)")

            if trip_list:
                embed.add_field(
                    name="🕐 Recent Trips",
                    value='\n'.join(trip_list),
                    inline=False
                )

            embed.set_footer(text="Data from Bolt Fleet API")

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Recent trips command failed: {e}")
            await ctx.followup.send(f"❌ Failed to fetch trip data: {str(e)}", ephemeral=True)

    @commands.hybrid_command(name="earnings", description="Get earnings information")
    @discord.app_commands.describe(days="Number of days to analyze (default: 30)")
    async def earnings(self, ctx, days: Optional[int] = 30):
        """Display earnings information"""
        try:
            await ctx.defer()

            if days < 1 or days > 90:
                await ctx.followup.send("❌ Days must be between 1 and 90", ephemeral=True)
                return

            start_date = datetime.now() - timedelta(days=days)

            async with self.bolt_client:
                earnings_data = await self.bolt_client.get_earnings_data(start_date=start_date)

            embed = discord.Embed(
                title=f"💰 Earnings Report ({days} days)",
                color=0x00d4aa,
                timestamp=datetime.now()
            )

            # Main earnings information
            gross_earnings = earnings_data.get('gross_earnings', 0)
            net_earnings = earnings_data.get('net_earnings', 0)
            bolt_fee = earnings_data.get('bolt_fee', 0)
            total_trips = earnings_data.get('total_trips', 0)

            embed.add_field(name="💵 Gross Earnings", value=f"€{gross_earnings:.2f}", inline=True)
            embed.add_field(name="🏦 Net Earnings", value=f"€{net_earnings:.2f}", inline=True)
            embed.add_field(name="🏢 Bolt Fee", value=f"€{bolt_fee:.2f}", inline=True)

            if total_trips > 0:
                avg_per_trip = gross_earnings / total_trips
                embed.add_field(name="📊 Trips Count", value=str(total_trips), inline=True)
                embed.add_field(name="📈 Avg per Trip", value=f"€{avg_per_trip:.2f}", inline=True)

            # Weekly breakdown if available
            if 'weekly_breakdown' in earnings_data:
                weekly_text = []
                for week in earnings_data['weekly_breakdown'][-4:]:  # Last 4 weeks
                    week_start = week.get('week_start', 'Unknown')
                    week_earnings = week.get('earnings', 0)
                    weekly_text.append(f"• Week of {week_start}: €{week_earnings:.2f}")

                if weekly_text:
                    embed.add_field(
                        name="📅 Weekly Breakdown",
                        value='\n'.join(weekly_text),
                        inline=False
                    )

            embed.set_footer(text="Data from Bolt Fleet API")

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Earnings command failed: {e}")
            await ctx.followup.send(f"❌ Failed to fetch earnings data: {str(e)}", ephemeral=True)

    @commands.hybrid_command(name="driver-stats", description="Get driver performance statistics")
    async def driver_stats(self, ctx):
        """Display driver performance statistics"""
        try:
            await ctx.defer()

            async with self.bolt_client:
                drivers = await self.bolt_client.get_driver_performance()

            if not drivers:
                await ctx.followup.send("No driver data available.")
                return

            embed = discord.Embed(
                title="👥 Driver Performance",
                description=f"Performance data for {len(drivers)} drivers",
                color=0xff9500,
                timestamp=datetime.now()
            )

            # Sort drivers by rating
            sorted_drivers = sorted(drivers, key=lambda x: x.get('rating', 0), reverse=True)

            # Top performers
            top_drivers = []
            for driver in sorted_drivers[:5]:
                name = driver.get('name', 'Unknown')
                rating = driver.get('rating', 0)
                trips = driver.get('total_trips', 0)
                earnings = driver.get('total_earnings', 0)
                top_drivers.append(f"⭐ **{name}** ({rating:.2f}★) - {trips} trips, €{earnings:.0f}")

            if top_drivers:
                embed.add_field(
                    name="🏆 Top Performers",
                    value='\n'.join(top_drivers),
                    inline=False
                )

            # Overall statistics
            total_earnings = sum(driver.get('total_earnings', 0) for driver in drivers)
            total_trips = sum(driver.get('total_trips', 0) for driver in drivers)
            avg_rating = sum(driver.get('rating', 0) for driver in drivers) / len(drivers) if drivers else 0

            embed.add_field(name="💰 Total Earnings", value=f"€{total_earnings:.2f}", inline=True)
            embed.add_field(name="🚗 Total Trips", value=str(total_trips), inline=True)
            embed.add_field(name="⭐ Avg Rating", value=f"{avg_rating:.2f}", inline=True)

            embed.set_footer(text="Data from Bolt Fleet API")

            await ctx.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Driver stats command failed: {e}")
            await ctx.followup.send(f"❌ Failed to fetch driver statistics: {str(e)}", ephemeral=True)


async def setup(bot):
    await bot.add_cog(FleetCommands(bot))