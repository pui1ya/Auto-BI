"""
Market basket analysis – find products/categories frequently bought together.
Uses the Apriori algorithm from mlxtend.
"""
import pandas as pd
from app.analytics.kpi import load_transactions

try:
    from mlxtend.frequent_patterns import apriori, association_rules
    from mlxtend.preprocessing import TransactionEncoder
    MLXTEND_AVAILABLE = True
except ImportError:
    MLXTEND_AVAILABLE = False


def basket_analysis(min_support: float = 0.02, min_lift: float = 1.0) -> pd.DataFrame:
    if not MLXTEND_AVAILABLE:
        return pd.DataFrame({"note": ["mlxtend not installed"]})

    df = load_transactions()
    if df.empty:
        return pd.DataFrame()

    # Build basket: each order = set of categories purchased
    basket = df.groupby(["transaction_id", "category"])["quantity"].sum().unstack().fillna(0)
    basket_bool = basket.map(lambda x: x > 0)

    if basket_bool.shape[0] < 5:
        return pd.DataFrame()

    freq_items = apriori(basket_bool, min_support=min_support, use_colnames=True)
    if freq_items.empty:
        return pd.DataFrame()

    rules = association_rules(freq_items, metric="lift", min_threshold=min_lift)
    rules = rules.sort_values("lift", ascending=False).head(20)
    rules["antecedents"] = rules["antecedents"].apply(lambda x: ", ".join(list(x)))
    rules["consequents"] = rules["consequents"].apply(lambda x: ", ".join(list(x)))
    rules = rules[["antecedents", "consequents", "support", "confidence", "lift"]].round(4)
    return rules.reset_index(drop=True)