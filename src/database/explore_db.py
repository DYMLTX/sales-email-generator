"""CLI tool to explore Azure database and understand the data structure."""

import click
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich import print as rprint
import logging

from src.database.azure_connector import AzureDBConnector, ProspectDataAccess

console = Console()
logging.basicConfig(level=logging.INFO)


@click.group()
def cli():
    """MAX.Live Database Explorer - Understand your prospect data."""
    pass


@cli.command()
def test_connection():
    """Test the Azure database connection."""
    console.print("[bold blue]Testing Azure Database Connection...[/bold blue]")
    
    db = AzureDBConnector()
    if db.test_connection():
        console.print("[bold green]✓ Connection successful![/bold green]")
    else:
        console.print("[bold red]✗ Connection failed![/bold red]")


@cli.command()
def list_tables():
    """List all tables in the database."""
    db = AzureDBConnector()
    
    try:
        tables = db.list_tables()
        
        table = Table(title="Available Tables")
        table.add_column("Table Name", style="cyan")
        table.add_column("Row Count", style="green")
        
        for table_name in tables:
            try:
                count = db.get_row_count(table_name)
                table.add_row(table_name, f"{count:,}")
            except Exception as e:
                table.add_row(table_name, "Error")
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]Error listing tables: {e}[/bold red]")


@cli.command()
@click.argument('table_name')
def describe_table(table_name):
    """Show schema for a specific table."""
    db = AzureDBConnector()
    
    try:
        schema = db.get_table_schema(table_name)
        
        table = Table(title=f"Schema for {table_name}")
        table.add_column("Column", style="cyan")
        table.add_column("Type", style="green")
        table.add_column("Max Length", style="yellow")
        table.add_column("Nullable", style="magenta")
        
        for _, row in schema.iterrows():
            table.add_row(
                row['COLUMN_NAME'],
                row['DATA_TYPE'],
                str(row['CHARACTER_MAXIMUM_LENGTH']) if row['CHARACTER_MAXIMUM_LENGTH'] else '-',
                '✓' if row['IS_NULLABLE'] == 'YES' else '✗'
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]Error describing table: {e}[/bold red]")


@cli.command()
@click.argument('table_name')
@click.option('--rows', default=10, help='Number of rows to display')
def sample_data(table_name, rows):
    """Show sample data from a table."""
    db = AzureDBConnector()
    
    try:
        data = db.sample_data(table_name, n=rows)
        
        # Create a pretty table
        table = Table(title=f"Sample data from {table_name}")
        
        # Add columns
        for col in data.columns:
            table.add_column(col, style="cyan", overflow="fold")
        
        # Add rows
        for _, row in data.iterrows():
            table.add_row(*[str(val) for val in row.values])
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]Error sampling data: {e}[/bold red]")


@cli.command()
def analyze_prospects():
    """Analyze prospect data quality and statistics."""
    console.print("[bold blue]Analyzing Prospect Data Quality...[/bold blue]")
    
    prospect_db = ProspectDataAccess()
    
    try:
        analysis = prospect_db.analyze_data_quality()
        
        # Contacts analysis
        contacts = analysis['contacts']
        console.print("\n[bold cyan]Contacts Analysis:[/bold cyan]")
        console.print(f"Total Contacts: [green]{contacts['total_contacts']:,}[/green]")
        console.print(f"Unique Emails: [green]{contacts['unique_emails']:,}[/green]")
        console.print(f"With Email: [green]{contacts['with_email']:,}[/green] ({contacts['with_email']/contacts['total_contacts']*100:.1f}%)")
        console.print(f"With Phone: [green]{contacts['with_phone']:,}[/green] ({contacts['with_phone']/contacts['total_contacts']*100:.1f}%)")
        console.print(f"With Account: [green]{contacts['with_account']:,}[/green] ({contacts['with_account']/contacts['total_contacts']*100:.1f}%)")
        
        # Accounts analysis
        accounts = analysis['accounts']
        console.print("\n[bold cyan]Accounts Analysis:[/bold cyan]")
        console.print(f"Total Accounts: [green]{accounts['total_accounts']:,}[/green]")
        console.print(f"With Industry: [green]{accounts['with_industry']:,}[/green] ({accounts['with_industry']/accounts['total_accounts']*100:.1f}%)")
        console.print(f"With Employee Count: [green]{accounts['with_employee_count']:,}[/green] ({accounts['with_employee_count']/accounts['total_accounts']*100:.1f}%)")
        console.print(f"With Revenue: [green]{accounts['with_revenue']:,}[/green] ({accounts['with_revenue']/accounts['total_accounts']*100:.1f}%)")
        
    except Exception as e:
        console.print(f"[bold red]Error analyzing data: {e}[/bold red]")


@cli.command()
def find_music_sponsors():
    """Find companies that might be interested in music sponsorships."""
    db = AzureDBConnector()
    
    console.print("[bold blue]Searching for potential music sponsors...[/bold blue]")
    
    # Look for companies in relevant industries
    query = """
    SELECT TOP 20
        a.name as company_name,
        a.industry,
        a.employee_count,
        a.annual_revenue,
        COUNT(c.id) as contact_count
    FROM accounts a
    LEFT JOIN contacts c ON a.id = c.account_id
    WHERE a.industry IN (
        'Entertainment', 'Media', 'Consumer Goods', 'Retail', 
        'Technology', 'Automotive', 'Beverage', 'Fashion',
        'Sports', 'Gaming', 'Hospitality'
    )
    AND a.employee_count > 100
    GROUP BY a.name, a.industry, a.employee_count, a.annual_revenue
    ORDER BY a.annual_revenue DESC
    """
    
    try:
        results = db.query_to_dataframe(query)
        
        table = Table(title="Potential Music Sponsors")
        table.add_column("Company", style="cyan")
        table.add_column("Industry", style="green")
        table.add_column("Employees", style="yellow")
        table.add_column("Revenue", style="magenta")
        table.add_column("Contacts", style="blue")
        
        for _, row in results.iterrows():
            table.add_row(
                row['company_name'],
                row['industry'],
                f"{row['employee_count']:,}" if pd.notna(row['employee_count']) else '-',
                f"${row['annual_revenue']:,.0f}" if pd.notna(row['annual_revenue']) else '-',
                str(row['contact_count'])
            )
        
        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]Error finding sponsors: {e}[/bold red]")


if __name__ == "__main__":
    cli()