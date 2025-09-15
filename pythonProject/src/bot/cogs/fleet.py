import discord
from discord.ext import commands
from discord import ui
from datetime import datetime, timedelta, date
from typing import Optional, List, Tuple
import calendar
import logging

logger = logging.getLogger(__name__)

# Company start date - no data before this
COMPANY_START_DATE = datetime(2025, 7, 28)


def in_channel(channel_ids: list[int]):
    def predicate(ctx):
        return ctx.channel.id in channel_ids
    return commands.check(predicate)


class CalendarNavigationView(ui.View):
    """Base view for calendar navigation"""

    def __init__(self, callback_func, driver_uuid: str = None, driver_name: str = None, timeout: int = 180):
        super().__init__(timeout=timeout)
        self.callback_func = callback_func
        self.driver_uuid = driver_uuid
        self.driver_name = driver_name
        self.current_date = datetime.now()

    def check_date_limits(self, check_date: datetime) -> bool:
        """Check if date is within valid range"""
        return COMPANY_START_DATE <= check_date <= datetime.now()


class InitialSelectView(CalendarNavigationView):
    """Initial view for selecting time period type"""

    @ui.button(label="📅 Day", style=discord.ButtonStyle.primary, row=0)
    async def day_view(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        view = DaySelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)

    @ui.button(label="📆 Week", style=discord.ButtonStyle.primary, row=0)
    async def week_view(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        view = WeekSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)

    @ui.button(label="📊 Month", style=discord.ButtonStyle.primary, row=0)
    async def month_view(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        view = MonthSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)

    @ui.button(label="📈 Year", style=discord.ButtonStyle.primary, row=0)
    async def year_view(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        view = YearSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)

    @ui.button(label="🎯 Custom", style=discord.ButtonStyle.success, row=0)
    async def custom_view(self, interaction: discord.Interaction, button: ui.Button):
        await interaction.response.defer()
        view = CustomDateSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)


class DaySelectView(CalendarNavigationView):
    """Week-based day selection view"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_week_start = self._get_week_start(datetime.now())
        self._update_buttons()

    def _get_week_start(self, dt: datetime) -> datetime:
        """Get Monday of the week for given date"""
        days_since_monday = dt.weekday()
        return dt - timedelta(days=days_since_monday)

    def _update_buttons(self):
        """Update button labels and states based on current week"""
        self.clear_items()

        # Navigation row
        prev_week = self.current_week_start - timedelta(days=7)
        next_week = self.current_week_start + timedelta(days=7)

        # Previous week button
        prev_button = ui.Button(label="◀", style=discord.ButtonStyle.secondary, row=0)
        prev_button.disabled = not self.check_date_limits(prev_week)
        prev_button.callback = self.prev_week
        self.add_item(prev_button)

        # Week range label
        week_end = self.current_week_start + timedelta(days=6)
        week_label = f"{self.current_week_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"
        label_button = ui.Button(label=week_label, style=discord.ButtonStyle.secondary, row=0, disabled=True)
        self.add_item(label_button)

        # Next week button
        next_button = ui.Button(label="▶", style=discord.ButtonStyle.secondary, row=0)
        next_button.disabled = not self.check_date_limits(next_week)
        next_button.callback = self.next_week
        self.add_item(next_button)

        # Day buttons (2 rows, Mon-Thu and Fri-Sun)
        for i in range(7):
            day_date = self.current_week_start + timedelta(days=i)
            day_name = day_date.strftime('%a')
            day_num = day_date.strftime('%d')

            # Determine button style
            if day_date.date() == datetime.now().date():
                style = discord.ButtonStyle.success  # Today is green
            elif not self.check_date_limits(day_date):
                style = discord.ButtonStyle.secondary  # Future/past dates
            else:
                style = discord.ButtonStyle.primary

            # Place Mon-Thu on row 1, Fri-Sun on row 2
            row = 1 if i < 4 else 2

            button = ui.Button(
                label=f"{day_name}\n{day_num}",
                style=style,
                row=row,
                disabled=not self.check_date_limits(day_date)
            )
            button.callback = self._make_day_callback(day_date)
            self.add_item(button)

        # Back button
        back_button = ui.Button(label="🔙 Back", style=discord.ButtonStyle.danger, row=3)
        back_button.callback = self.go_back
        self.add_item(back_button)

    def _make_day_callback(self, day_date: datetime):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()
            # Calculate stats for the specific day
            await self.callback_func(interaction, day_date, self.driver_uuid, view_type='day')
            self.stop()

        return callback

    async def prev_week(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_week_start -= timedelta(days=7)
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def next_week(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_week_start += timedelta(days=7)
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def go_back(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = InitialSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)


class WeekSelectView(CalendarNavigationView):
    """View for selecting weeks"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_month = datetime.now().replace(day=1)
        self._update_buttons()

    def _get_weeks_in_month(self, month: datetime) -> List[Tuple[datetime, datetime]]:
        """Get all weeks (Mon-Sun) that overlap with the month"""
        weeks = []

        # Start from first day of month
        first_day = month.replace(day=1)
        # Find the Monday of that week
        days_to_monday = first_day.weekday()
        week_start = first_day - timedelta(days=days_to_monday)

        # Get last day of month
        last_day = month.replace(day=calendar.monthrange(month.year, month.month)[1])

        # Collect all weeks that overlap with this month
        while week_start <= last_day:
            week_end = week_start + timedelta(days=6)
            weeks.append((week_start, week_end))
            week_start += timedelta(days=7)

        return weeks

    def _update_buttons(self):
        """Update buttons for week selection"""
        self.clear_items()

        # Navigation row
        prev_month = (self.current_month - timedelta(days=1)).replace(day=1)
        next_month = (self.current_month + timedelta(days=32)).replace(day=1)

        # Previous month button
        prev_button = ui.Button(label="◀", style=discord.ButtonStyle.secondary, row=0)
        prev_button.disabled = not self.check_date_limits(prev_month)
        prev_button.callback = self.prev_month
        self.add_item(prev_button)

        # Month label
        month_label = self.current_month.strftime('%B %Y')
        label_button = ui.Button(label=month_label, style=discord.ButtonStyle.secondary, row=0, disabled=True)
        self.add_item(label_button)

        # Next month button
        next_button = ui.Button(label="▶", style=discord.ButtonStyle.secondary, row=0)
        next_button.disabled = not self.check_date_limits(next_month)
        next_button.callback = self.next_month
        self.add_item(next_button)

        # Week buttons
        weeks = self._get_weeks_in_month(self.current_month)
        for i, (week_start, week_end) in enumerate(weeks[:4]):  # Max 4 weeks shown
            week_label = f"{week_start.strftime('%b %d')} - {week_end.strftime('%b %d')}"

            # Check if current week
            now = datetime.now()
            if week_start <= now <= week_end:
                style = discord.ButtonStyle.success
            else:
                style = discord.ButtonStyle.primary

            button = ui.Button(label=week_label, style=style, row=i // 2 + 1)
            button.disabled = not self.check_date_limits(week_start)
            button.callback = self._make_week_callback(week_start, week_end)
            self.add_item(button)

        # Back button
        back_button = ui.Button(label="🔙 Back", style=discord.ButtonStyle.danger, row=3)
        back_button.callback = self.go_back
        self.add_item(back_button)

    def _make_week_callback(self, week_start: datetime, week_end: datetime):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()
            await self.callback_func(interaction, (week_start, week_end), self.driver_uuid, view_type='week')
            self.stop()

        return callback

    async def prev_month(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_month = (self.current_month - timedelta(days=1)).replace(day=1)
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def next_month(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_month = (self.current_month + timedelta(days=32)).replace(day=1)
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def go_back(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = InitialSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)


class MonthSelectView(CalendarNavigationView):
    """View for selecting months"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.current_year = datetime.now().year
        self._update_buttons()

    def _update_buttons(self):
        """Update buttons for month selection"""
        self.clear_items()

        # Year navigation
        prev_button = ui.Button(label="◀", style=discord.ButtonStyle.secondary, row=0)
        prev_button.disabled = self.current_year <= COMPANY_START_DATE.year
        prev_button.callback = self.prev_year
        self.add_item(prev_button)

        year_label = ui.Button(label=str(self.current_year), style=discord.ButtonStyle.secondary, row=0, disabled=True)
        self.add_item(year_label)

        next_button = ui.Button(label="▶", style=discord.ButtonStyle.secondary, row=0)
        next_button.disabled = self.current_year >= datetime.now().year
        next_button.callback = self.next_year
        self.add_item(next_button)

        # Month buttons (3x4 grid)
        for i in range(12):
            month_date = datetime(self.current_year, i + 1, 1)
            month_name = month_date.strftime('%B')

            # Determine style
            if month_date.year == datetime.now().year and month_date.month == datetime.now().month:
                style = discord.ButtonStyle.success
            else:
                style = discord.ButtonStyle.primary

            row = (i // 4) + 1  # 3 rows of 4 months
            button = ui.Button(label=month_name[:3], style=style, row=row)
            button.disabled = not self.check_date_limits(month_date)
            button.callback = self._make_month_callback(month_date)
            self.add_item(button)

        # Back button
        back_button = ui.Button(label="🔙 Back", style=discord.ButtonStyle.danger, row=4)
        back_button.callback = self.go_back
        self.add_item(back_button)

    def _make_month_callback(self, month_date: datetime):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()
            await self.callback_func(interaction, month_date, self.driver_uuid, view_type='month')
            self.stop()

        return callback

    async def prev_year(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_year -= 1
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def next_year(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_year += 1
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def go_back(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = InitialSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)


class YearSelectView(CalendarNavigationView):
    """View for selecting years"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._update_buttons()

    def _update_buttons(self):
        """Update buttons for year selection"""
        self.clear_items()

        # Calculate available years
        start_year = COMPANY_START_DATE.year
        current_year = datetime.now().year

        # Year buttons
        for year in range(start_year, current_year + 1):
            style = discord.ButtonStyle.success if year == current_year else discord.ButtonStyle.primary
            button = ui.Button(label=str(year), style=style, row=0)
            button.callback = self._make_year_callback(year)
            self.add_item(button)

        # Back button
        back_button = ui.Button(label="🔙 Back", style=discord.ButtonStyle.danger, row=1)
        back_button.callback = self.go_back
        self.add_item(back_button)

    def _make_year_callback(self, year: int):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()
            year_date = datetime(year, 1, 1)
            await self.callback_func(interaction, year_date, self.driver_uuid, view_type='year')
            self.stop()

        return callback

    async def go_back(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = InitialSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)


class CustomDateSelectView(CalendarNavigationView):
    """View for custom date range selection"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_date = None
        self.end_date = None
        self.selecting_end = False
        self.current_week_start = self._get_week_start(datetime.now())
        self._update_buttons()

    def _get_week_start(self, dt: datetime) -> datetime:
        """Get Monday of the week for given date"""
        days_since_monday = dt.weekday()
        return dt - timedelta(days=days_since_monday)

    def _update_buttons(self):
        """Update buttons for custom date selection"""
        self.clear_items()

        # Status label
        if not self.selecting_end:
            status_text = "📅 Select START Date"
        else:
            status_text = f"📅 Select END Date (Start: {self.start_date.strftime('%b %d, %Y')})"

        status_button = ui.Button(label=status_text, style=discord.ButtonStyle.secondary, row=0, disabled=True)
        self.add_item(status_button)

        # Navigation row
        prev_week = self.current_week_start - timedelta(days=7)
        next_week = self.current_week_start + timedelta(days=7)

        # Previous week button
        prev_button = ui.Button(label="◀", style=discord.ButtonStyle.secondary, row=1)
        prev_button.disabled = not self.check_date_limits(prev_week)
        prev_button.callback = self.prev_week
        self.add_item(prev_button)

        # Week range label
        week_end = self.current_week_start + timedelta(days=6)
        week_label = f"{self.current_week_start.strftime('%b %d')} - {week_end.strftime('%b %d')}"
        label_button = ui.Button(label=week_label, style=discord.ButtonStyle.secondary, row=1, disabled=True)
        self.add_item(label_button)

        # Next week button
        next_button = ui.Button(label="▶", style=discord.ButtonStyle.secondary, row=1)
        next_button.disabled = not self.check_date_limits(next_week)
        next_button.callback = self.next_week
        self.add_item(next_button)

        # Day buttons
        for i in range(7):
            day_date = self.current_week_start + timedelta(days=i)
            day_name = day_date.strftime('%a')
            day_num = day_date.strftime('%d')

            # Determine button style
            if self.start_date and day_date.date() == self.start_date.date():
                style = discord.ButtonStyle.success  # Selected start date
            elif day_date.date() == datetime.now().date():
                style = discord.ButtonStyle.primary
            else:
                style = discord.ButtonStyle.secondary

            # Disable dates before start date when selecting end
            disabled = not self.check_date_limits(day_date)
            if self.selecting_end and self.start_date and day_date < self.start_date:
                disabled = True

            row = 2 if i < 4 else 3

            button = ui.Button(
                label=f"{day_name}\n{day_num}",
                style=style,
                row=row,
                disabled=disabled
            )
            button.callback = self._make_day_callback(day_date)
            self.add_item(button)

        # Back/Cancel button
        back_button = ui.Button(label="🔙 Back", style=discord.ButtonStyle.danger, row=4)
        back_button.callback = self.go_back
        self.add_item(back_button)

        # Reset button if we have a start date
        if self.start_date:
            reset_button = ui.Button(label="🔄 Reset", style=discord.ButtonStyle.secondary, row=4)
            reset_button.callback = self.reset_dates
            self.add_item(reset_button)

    def _make_day_callback(self, day_date: datetime):
        async def callback(interaction: discord.Interaction):
            await interaction.response.defer()

            if not self.selecting_end:
                # Selecting start date
                self.start_date = day_date
                self.selecting_end = True
                self._update_buttons()
                await interaction.edit_original_response(view=self)
            else:
                # Selecting end date
                self.end_date = day_date
                await self.callback_func(interaction, (self.start_date, self.end_date), self.driver_uuid,
                                         view_type='custom')
                self.stop()

        return callback

    async def prev_week(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_week_start -= timedelta(days=7)
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def next_week(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.current_week_start += timedelta(days=7)
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def reset_dates(self, interaction: discord.Interaction):
        await interaction.response.defer()
        self.start_date = None
        self.end_date = None
        self.selecting_end = False
        self._update_buttons()
        await interaction.edit_original_response(view=self)

    async def go_back(self, interaction: discord.Interaction):
        await interaction.response.defer()
        view = InitialSelectView(self.callback_func, self.driver_uuid, self.driver_name)
        await interaction.edit_original_response(view=view)


class FleetCommands(commands.Cog):
    """Enhanced fleet management commands with calendar interface"""

    def __init__(self, bot):
        self.bot = bot
        self.bolt_client = bot.bolt_client

    @commands.hybrid_command(name="help", description="Show all available commands")
    async def help_command(self, ctx):
        """Display all available commands with descriptions"""
        try:
            embed = discord.Embed(
                title="📚 DesiSquad Fleet Bot Commands",
                description="Here are all the available commands:",
                color=0x0099ff,
                timestamp=datetime.now()
            )

            # General Commands
            embed.add_field(
                name="📋 General Commands",
                value=(
                    "**!help** - Show this help message\n"
                    "**!sync** - Sync database with Bolt API (cooldown: 60s)\n"
                    "**!fleet-stats** - Display fleet overview statistics"
                ),
                inline=False
            )

            # Driver Commands
            embed.add_field(
                name="👥 Driver Commands",
                value=(
                    "**!drivers** - List all drivers with their numbers\n"
                    "**!driver-stats [number]** - Get detailed driver statistics with calendar selection"
                ),
                inline=False
            )

            # Company Commands
            embed.add_field(
                name="🏢 Company Commands",
                value=(
                    "**!company-earnings** - View company earnings with calendar selection"
                ),
                inline=False
            )

            # Admin Commands (if the user has admin permissions)
            if ctx.author.guild_permissions.administrator:
                embed.add_field(
                    name="⚙️ Admin Commands",
                    value=(
                        "**!set-report-channel [channel]** - Set channel for automated reports\n"
                        "**!test-midnight-report** - Test the daily report functionality\n"
                        "**!force-sync** - Force immediate database sync"
                    ),
                    inline=False
                )

            # Footer with additional info
            embed.set_footer(text="Use !command or /command for slash commands • Data updates every 10 minutes")

            if hasattr(ctx, 'followup'):
                await ctx.followup.send(embed=embed)
            else:
                await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Help command failed: {e}")
            await ctx.send("❌ Failed to display help information.")

    @commands.hybrid_command(name="company-earnings", description="Get company earnings with calendar selection")
    @commands.has_role("Admin")
    @in_channel([1415574639731802223])
    async def company_earnings(self, ctx):
        """Display company earnings with interactive calendar selection"""
        try:
            if hasattr(ctx, 'defer'):
                await ctx.defer()
            else:
                async with ctx.typing():
                    pass

            # Show initial selection view (reusing the same calendar navigation)
            view = InitialSelectView(self._show_company_earnings)

            embed = discord.Embed(
                title="💰 DesiSquad Company Earnings",
                description="Select a time period to view company earnings:",
                color=0x00d4aa
            )

            if hasattr(ctx, 'followup'):
                await ctx.followup.send(embed=embed, view=view)
            else:
                await ctx.send(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Company earnings command failed: {e}")
            await ctx.send(f"❌ Failed to fetch earnings: {str(e)}")

    async def _show_company_earnings(self, interaction: discord.Interaction, date_input, driver_uuid=None,
                                     view_type: str = None):
        """Show company earnings for selected period"""
        try:
            # Calculate date range based on view type
            if view_type == 'day':
                start_date = date_input.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date + timedelta(days=1)
                period_text = start_date.strftime('%B %d, %Y')
            elif view_type == 'week':
                start_date, end_date = date_input
                period_text = f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d, %Y')}"
            elif view_type == 'month':
                start_date = date_input.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                if start_date.month == 12:
                    end_date = start_date.replace(year=start_date.year + 1, month=1)
                else:
                    end_date = start_date.replace(month=start_date.month + 1)
                period_text = start_date.strftime('%B %Y')
            elif view_type == 'year':
                start_date = date_input.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                end_date = start_date.replace(year=start_date.year + 1)
                period_text = str(start_date.year)
            elif view_type == 'custom':
                start_date, end_date = date_input
                start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
                end_date = end_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                period_text = f"{start_date.strftime('%b %d')} - {(end_date - timedelta(days=1)).strftime('%b %d, %Y')}"
            else:
                raise ValueError(f"Unknown view type: {view_type}")

            # Get company earnings from database
            stats = self.bolt_client.db.get_company_earnings_by_date_range(start_date, end_date)

            if not stats or stats['trips_completed'] == 0:
                await interaction.followup.send("No data found for this period.")
                return

            # Create earnings embed
            embed = discord.Embed(
                title=f"💰 DesiSquad Earnings - {period_text}",
                color=0x00d4aa,
                timestamp=datetime.now()
            )

            # Earnings section
            embed.add_field(
                name="💵 Earnings",
                value=(
                    f"**Gross Earnings:** {stats['gross_earnings']} RON\n"
                    f"**Total Fees:** {stats['total_fees']} RON\n"
                    f"**Net Earnings:** {stats['net_earnings']} RON\n"
                    f"**💸 Cash Collected:** {stats['cash_collected']} RON"
                ),
                inline=False
            )

            # Trips section
            embed.add_field(
                name="📊 Trips",
                value=(
                    f"**Trips Completed:** {stats['trips_completed']}\n"
                    f"**Total Distance:** {stats['total_distance']} km\n"
                    f"**Average Distance:** {stats['avg_distance']} km/trip"
                ),
                inline=False
            )

            # Metrics section
            embed.add_field(
                name="📈 Performance Metrics",
                value=(
                    f"**Earnings/Trip:** {stats['earnings_per_trip']} RON\n"
                    f"**Earnings/KM:** {stats['earnings_per_km']} RON/km\n"
                    f"**Average Rating:** {stats['avg_rating']}/5 ⭐" if stats[
                                                                            'avg_rating'] > 0 else f"**Earnings/Trip:** {stats['earnings_per_trip']} RON\n**Earnings/KM:** {stats['earnings_per_km']} RON/km"
                ),
                inline=False
            )

            # Driver breakdown if available
            if stats.get('driver_breakdown'):
                driver_text = []
                for driver in stats['driver_breakdown'][:5]:  # Top 5 drivers
                    driver_text.append(f"**{driver['name']}:** {driver['trips']} trips, {driver['earnings']} RON")

                if driver_text:
                    embed.add_field(
                        name="🏆 Top Drivers",
                        value="\n".join(driver_text),
                        inline=False
                    )

            embed.set_footer(text=f"View Type: {view_type.capitalize()} • Data from local database")

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Failed to show company earnings: {e}")
            await interaction.followup.send(f"❌ Failed to fetch earnings: {str(e)}")

    @commands.hybrid_command(name="driver-stats", description="Get driver statistics with interactive calendar")
    async def driver_stats(self, ctx, driver_number: int):
        """Display driver statistics with interactive calendar selection"""
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

            # Show initial selection view
            view = InitialSelectView(self._show_driver_stats, driver_uuid, driver_name)

            embed = discord.Embed(
                title=f"📊 Driver Statistics - {driver_name}",
                description="Select a time period to view statistics:",
                color=0x0099ff
            )

            if hasattr(ctx, 'followup'):
                await ctx.followup.send(embed=embed, view=view)
            else:
                await ctx.send(embed=embed, view=view)

        except Exception as e:
            logger.error(f"Driver stats command failed: {e}")
            await ctx.send(f"❌ Failed to fetch driver stats: {str(e)}")

    async def _show_driver_stats(self, interaction: discord.Interaction, date_input, driver_uuid: str, view_type: str):
        """Show driver stats with complete time tracking (Romania-local periods, UTC queries)"""
        try:
            import pytz
            romania_tz = pytz.timezone("Europe/Bucharest")
            utc = pytz.UTC

            # Normalize input -> Romania tz
            def to_romania(d: datetime) -> datetime:
                if d.tzinfo is None:
                    return romania_tz.localize(d)
                return d.astimezone(romania_tz)

            # Calculate start_date and end_date (Romania), then convert to UTC
            if view_type == "day":
                local_start = to_romania(date_input).replace(hour=0, minute=0, second=0, microsecond=0)
                local_end = local_start + timedelta(days=1)
                period_text = local_start.strftime("%B %d, %Y")

            elif view_type == "week":
                week_start, week_end = date_input
                local_start = to_romania(week_start).replace(hour=0, minute=0, second=0, microsecond=0)
                local_end = to_romania(week_end).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)
                period_text = f"{local_start.strftime('%b %d')} - {week_end.strftime('%b %d, %Y')}"

            elif view_type == "month":
                local_start = to_romania(date_input).replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                if local_start.month == 12:
                    local_end = local_start.replace(year=local_start.year + 1, month=1)
                else:
                    local_end = local_start.replace(month=local_start.month + 1)
                period_text = local_start.strftime("%B %Y")

            elif view_type == "year":
                local_start = to_romania(date_input).replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                local_end = local_start.replace(year=local_start.year + 1)
                period_text = str(local_start.year)

            elif view_type == "custom":
                custom_start, custom_end = date_input
                local_start = to_romania(custom_start).replace(hour=0, minute=0, second=0, microsecond=0)
                local_end = to_romania(custom_end).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
                    days=1)
                period_text = f"{local_start.strftime('%b %d')} - {custom_end.strftime('%b %d, %Y')}"

            else:
                raise ValueError(f"Unknown view type: {view_type}")

            # Convert to UTC for querying
            start_date = local_start.astimezone(utc)
            end_date = local_end.astimezone(utc)

            logger.info(f"[{view_type}] period {period_text}")
            logger.info(f"Querying from {start_date} to {end_date} (UTC)")

            # Fetch state logs if within 31 days
            state_logs = []
            days_diff = (end_date - start_date).days
            if days_diff <= 31:
                async with self.bolt_client:
                    try:
                        state_response = await self.bolt_client.get_fleet_state_logs(
                            start_date=start_date,
                            end_date=end_date,
                            limit=1000
                        )
                        if state_response.get("code") == 0:
                            state_logs = state_response.get("data", {}).get("state_logs", [])
                            logger.info(f"Fetched {len(state_logs)} state logs")
                    except Exception as e:
                        logger.warning(f"Could not fetch state logs: {e}")
            else:
                logger.info(f"Period > 31 days ({days_diff}), skipping state logs fetch")

            # Get stats
            stats = self.bolt_client.db.get_driver_stats_by_date_range(
                driver_uuid,
                start_date,
                end_date,
                state_logs
            )

            if not stats:
                await interaction.followup.send(f"No data found for this period ({period_text}).")
                return

            # Build embed
            embed = discord.Embed(
                title=f"👤 {stats['driver_name']} - {period_text}",
                color=0xff9500,
                timestamp=datetime.now(utc)
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

            # Hours
            active_h = int(stats['hours_worked'])
            active_m = int((stats['hours_worked'] - active_h) * 60)
            total_h = int(stats['total_online_hours'])
            total_m = int((stats['total_online_hours'] - total_h) * 60)
            waiting_h = int(stats['waiting_hours'])
            waiting_m = int((stats['waiting_hours'] - waiting_h) * 60)

            embed.add_field(
                name="📍 Distance & Time",
                value=(
                    f"**Total Distance:** {stats['total_distance']} km\n"
                    f"**Avg Distance/Trip:** {stats['avg_distance']} km\n"
                    f"**Total Online:** {total_h}h {total_m}m\n"
                    f"├─ **Active:** {active_h}h {active_m}m\n"
                    f"└─ **Waiting:** {waiting_h}h {waiting_m}m"
                ),
                inline=False
            )

            # Performance metrics
            net_per_hour_total = stats['net_earnings'] / stats['total_online_hours'] if stats[
                                                                                            'total_online_hours'] > 0 else 0
            net_per_hour_active = stats['net_earnings'] / stats['hours_worked'] if stats['hours_worked'] > 0 else 0

            embed.add_field(
                name="💰 Performance Metrics",
                value=(
                    f"**Gross Earnings/Hour:** {stats['earnings_per_hour']:.2f} Total | {stats['earnings_per_hour_active']:.2f} Active\n"
                    f"**Net Earnings/Hour:** {net_per_hour_total:.2f} Total | {net_per_hour_active:.2f} Active\n"
                    f"**Earnings/KM:** {stats['earnings_per_km']} RON/km"
                ),
                inline=False
            )

            # Footer
            footer_text = f"View Type: {view_type.capitalize()} | Romania time (local)"
            if stats['waiting_hours'] > 0 and not state_logs:
                footer_text += " | Waiting time estimated from order gaps"
            elif state_logs:
                footer_text += " | Time data from driver state logs"

            embed.set_footer(text=footer_text)

            await interaction.followup.send(embed=embed)

        except Exception as e:
            logger.error(f"Failed to show driver stats: {e}")
            await interaction.followup.send(f"❌ Failed to fetch statistics: {str(e)}")

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

            embed.add_field(
                name="📊 Last 7 Days",
                value=(
                    f"**Trips Completed:** {week_stats['total_trips']}\n"
                    f"**Distance Traveled:** {week_stats['total_distance_km']} km"
                ),
                inline=True
            )

            embed.add_field(
                name="📈 All Time",
                value=(
                    f"**Trips Completed:** {all_time_stats['total_trips']}\n"
                    f"**Distance Traveled:** {all_time_stats['total_distance_km']} km"
                ),
                inline=True
            )

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

            embed.set_footer(text="Use: !driver-stats [number]")

            if hasattr(ctx, 'followup'):
                await ctx.followup.send(embed=embed)
            else:
                await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Drivers list command failed: {e}")
            await ctx.send(f"❌ Failed to fetch drivers: {str(e)}")

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


async def setup(bot):
    await bot.add_cog(FleetCommands(bot))