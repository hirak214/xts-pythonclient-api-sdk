import numpy as np
from scipy.stats import norm

# Define Black-Scholes formula
def black_scholes(S, K, T, r, sigma, option_type="call"):
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    if option_type == "call":
        price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    elif option_type == "put":
        price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
    return price

# Simulate data for 20 stocks
stock_data = [
    {"symbol": f"STOCK{i+1}", "S": np.random.uniform(50, 150), "IV": np.random.uniform(0.2, 0.5)} 
    for i in range(20)
]

# Define parameters
K = 100  # Strike price (use a common strike price for simplicity)
T = 30 / 365  # 30 days to expiration
r = 0.02  # Risk-free rate

# Calculate scores for each stock
selected_stocks = []
for stock in stock_data:
    S = stock["S"]
    sigma = stock["IV"]
    
    # Calculate theoretical call price using Black-Scholes
    theoretical_call_price = black_scholes(S, K, T, r, sigma, option_type="call")
    
    # Simulate market price (randomly for example purposes)
    market_price = theoretical_call_price * np.random.uniform(0.9, 1.1)
    
    # Calculate mispricing
    mispricing = abs(theoretical_call_price - market_price)
    
    # Calculate Greeks (Delta, Vega, Theta for demonstration)
    d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    delta = norm.cdf(d1)  # Delta
    vega = S * norm.pdf(d1) * np.sqrt(T)  # Vega
    theta = - (S * norm.pdf(d1) * sigma) / (2 * np.sqrt(T))  # Theta

    # Score based on IV, Vega, and mispricing
    score = (sigma * 100) + (vega * 10) + (mispricing * 5)
    selected_stocks.append({"symbol": stock["symbol"], "score": score, "IV": sigma, "delta": delta, 
                            "vega": vega, "theta": theta, "mispricing": mispricing})

# Sort stocks by score and pick top 5
top_5_stocks = sorted(selected_stocks, key=lambda x: x["score"], reverse=True)[:5]

# Display top 5 stocks
print("Top 5 Stocks for Strategy:")
for stock in top_5_stocks:
    print(f"Symbol: {stock['symbol']}, Score: {stock['score']:.2f}, IV: {stock['IV']:.2f}, "
          f"Delta: {stock['delta']:.2f}, Vega: {stock['vega']:.2f}, Theta: {stock['theta']:.2f}, "
          f"Mispricing: {stock['mispricing']:.2f}")
