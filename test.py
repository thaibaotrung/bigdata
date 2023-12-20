import pandas as pd
import matplotlib.pyplot as plt

# Stock market data
data = {
    "Symbol": ["3D SYSTEMS CORP.", "3M COMPANY", "A.K.A. BRANDS HOLDING", "A.O. SMITH CORP.", "A10 NETWORKS INC.",
               "AAR CORP.", "AARONS HOLDINGS CO.", "ABBOTT LABORATORIES", "ABBVIE INC.", "ABERCROMBIE & FITCH CO.",
               "ABM INDUSTRIES INC.", "ABRDN GLOB. DYNAMIC DIVIDEND FUND", "ABRDN GLOB. INFRA. INC. FUND",
               "ABRDN GLOB. PREMIER PROPERTIES FUND", "ABRDN HEALTHCARE INVESTORS SHARES OF BE",
               "ABRDN HEALTHCARE OPPORTUNITIES FUND SHA", "ABRDN INC. CREDIT STRATEGIES FUND",
               "ABRDN INC. CREDIT STRATEGIES FUND 5.250", "ABRDN LIFE SCIENCES INVESTORS SHARES OF"],
    "Last": [5.93, 103.28, 9.80, 78.01, 12.56, 69.32, 10.46, 104.05, 147.97, 77.94,
             43.60, 9.18, 17.32, 3.68, 15.43, 17.49, 6.51, 22.74, 12.44],
}

df = pd.DataFrame(data)

# Plotting
plt.figure(figsize=(12, 6))
plt.bar(df['Symbol'], df['Last'], color='blue')
plt.xlabel('Symbol')
plt.ylabel('Last Price')
plt.title('Stock Prices')
plt.xticks(rotation=90)
plt.tight_layout()

# Show the plot
plt.show()
