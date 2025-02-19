<h2>Test Project about Demand Forecasting for a Manufacturing company.</h2>

<b>Components:</b>
- using GitHub Codespace for development
- using Epicor tables for historical Job data
- using SQLAlchemy to read/write data from SQL Server
- using Python Prophet library for forecasting

<b>Process:</b>
1. Get open Jobs and their required components and look for them in following sequence
    - in inventory
    - purchase orders
    - ongoing jobs
2. If found in any source subtract the quantity along with receiving date (in case of PO or Jobs)
3. If not found then store the list of components which needs to be ordered

<b>Enhancement:</b></br>
- Add cost to the process to determine purchasing targets
- Create purchase orders for components which are not found in any source also decide which supplier is suitable considering quality and delivery time.
- Create visualization to show the trends

