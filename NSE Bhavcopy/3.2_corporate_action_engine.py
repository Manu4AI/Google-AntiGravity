
import logging

class CorporateActionEngine:
    """
    Unified Python engine to calculate correct, exchange-grade logic for 
    SPLIT, BONUS, RIGHTS, and DEMERGER.
    
    Based on User Specifications:
    
    1. STOCK SPLIT
       - Price: Reduces proportionally (Multiplier = New shares / Old shares ? No, Old/New).
         User Logic: Price Multiplier = B / A ?? 
         Let's stick to the Example: 1:2 Split (1 become 2). Price 100 -> 50.
         Factor to multiply Old Price to get New Price? No, factor to multiply HISTORICAL price.
         Standard Adjustment: Historical Price * Factor = Adjusted Price.
         If today price is 50, and yesterday was 100. We want yesterday to look like 50.
         So Factor = 0.5.
         Formula: Old Shares / New Shares. (1/2 = 0.5).
       - Quantity: Increases proportionally.
    
    2. BONUS ISSUE
       - Bonus A:B (A bonus for B held? User says 'Bonus 1:1', A=held, B=bonus).
         User: A=Held, B=Bonus.
         Price Multiplier = A / (A + B).
         Example: 1:1 Bonus. 1 held, 1 bonus. Total 2. Factor = 1 / 2 = 0.5.
         
    3. RIGHTS ISSUE
       - A = shares held, B = rights shares.
       - P = cum-rights market price.
       - R = rights issue price.
       - TERP = ((A * P) + (B * R)) / (A + B)
       - Price Multiplier = TERP / P
       
    4. DEMERGER
       - no universal price formula.
       - Returns price_multiplier = 1.0 (No adjustment) by default as per instruction.
       - Calculates Cost Split.
    """

    @staticmethod
    def parse_ratio(ratio_str):
        """
        Parses ratio string "A:B" into floats (a, b).
        Handles formatting errors.
        """
        try:
            parts = ratio_str.split(':')
            return float(parts[0]), float(parts[1])
        except (ValueError, IndexError):
            logging.error(f"Invalid ratio format: {ratio_str}")
            return None, None

    @staticmethod
    def calculate_split(ratio, current_price=None, current_qty=None):
        """
        SPLIT Logic
        Ratio format assumption: "Old:New" (e.g., 1:10 for 1 share becoming 10, or 10:1 if FV changes? Standard is Held:Received or simply Old:New share count)
        User Example: "Split from FV 10 to FV 5 -> 1:2". Old=1, New=2.
        Input Ratio: 1:2
        
        Outputs:
        - price_multiplier: Factor to adjust historical prices.
        - qty_multiplier: Factor to adjust current holdings.
        """
        a, b = CorporateActionEngine.parse_ratio(ratio) # A=Old, B=New
        if not a or not b: return {}

        # User: Price Multiplier = B / A (User text said B/A, but example 100->50 implies 0.5 which is A/B)
        # Let's trust the Example: Price 100 -> 50.
        # Adjusted = Original * Factor. 50 = 100 * Factor. Factor = 0.5.
        # Factor = Old / New = 1 / 2.
        
        price_multiplier = a / b
        qty_multiplier = b / a # Quantity increases
        
        return {
            'action': 'SPLIT',
            'price_multiplier': price_multiplier,
            'qty_multiplier': qty_multiplier,
            'description': f"Split {a}:{b}. Price x{price_multiplier:.4f}, Qty x{qty_multiplier:.4f}"
        }

    @staticmethod
    def calculate_bonus(ratio, current_price=None, current_qty=None):
        """
        BONUS Logic
        Ratio format: "Bonus:Held" (User Example: "Bonus 1:1", A=1 Held, B=1 Bonus)
        Wait, User Text: "A=shares held, B=bonus shares".
        User Example: "1:1". 
        Standard parsing: Usually "X:Y" means X bonus for Y held.
        User Input: "1:1". 
        
        Formula:
        Price Multiplier = A / (A + B) (Where A=Held, B=Bonus)
        """
        bonus, held = CorporateActionEngine.parse_ratio(ratio) # Bonus:Held
        if not bonus or not held: return {}
        
        b = bonus
        a = held
        
        # Price Multiplier = Held / (Held + Bonus)
        price_multiplier = a / (a + b)
        
        # Qty Multiplier = (Held + Bonus) / Held
        qty_multiplier = (a + b) / a
        
        return {
            'action': 'BONUS',
            'price_multiplier': price_multiplier,
            'qty_multiplier': qty_multiplier,
            'description': f"Bonus {bonus}:{held}. Price x{price_multiplier:.4f}, Qty x{qty_multiplier:.4f}"
        }

    @staticmethod
    def calculate_rights(ratio, issue_price, market_price_cum_rights):
        """
        RIGHTS ISSUE Logic
        Ratio: "Rights:Held" (User Example: "3:25")
        Issue Price (R)
        Market Price (P) - Cum-Rights (Last Close)
        
        TERP = ((A * P) + (B * R)) / (A + B)
        Price Multiplier = TERP / P
        """
        rights_shares, held_shares = CorporateActionEngine.parse_ratio(ratio)
        if not rights_shares or not held_shares: return {}
        
        b = rights_shares
        a = held_shares
        p = float(market_price_cum_rights)
        r = float(issue_price)
        
        terp = ((a * p) + (b * r)) / (a + b)
        
        price_multiplier = terp / p
        
        # Quantity only changes if subscribed. This engine provides the *factor* if fully subscribed?
        # User: "New Qty = Old Qty * (A+B)/A" (If subscribed)
        # We will return the potential qty multiplier for subscribers.
        qty_multiplier_subscribed = (a + b) / a
        
        return {
            'action': 'RIGHTS',
            'price_multiplier': price_multiplier,
            'qty_multiplier_subscribed': qty_multiplier_subscribed,
            'terp': terp,
            'description': f"Rights {b}:{a} @ {r}. TERP {terp:.2f}. Price Factor {price_multiplier:.4f}"
        }

    @staticmethod
    def calculate_demerger(ratio, cost_split_parent_pct=None):
        """
        DEMERGER Logic
        Ratio: "Held:Received" (Usually 1:1 or as defined)
        Price Multiplier: 1.0 (NO backward adjustment per user instruction)
        Cost Split: Calculated if percentage provided.
        """
        # Parsing ratio just in case it's needed for quantity of new entity
        # Format "Held:Received"? User Example: "1:1"
        try:
             h, r = CorporateActionEngine.parse_ratio(ratio)
        except:
             h, r = 1, 1

        price_multiplier = 1.0 # Explicit instruction: NO backward price multiplier
        
        result = {
            'action': 'DEMERGER',
            'price_multiplier': price_multiplier,
            'qty_multiplier_parent': 1.0, # Parent qty unchanged
            'new_entity_ratio': f"{r} for every {h} held", # Quantity of child
            'description': "Demerger. No Price Adj. Cost Split Required."
        }
        
        if cost_split_parent_pct is not None:
            # Pct should be 0-100
            x = float(cost_split_parent_pct)
            result['parent_cost_pct'] = x
            result['child_cost_pct'] = 100.0 - x
            
        return result

# --- Verification / Test Block ---
if __name__ == "__main__":
    print("--- Testing Corporate Action Engine with User Examples ---\n")
    
    # 1. Split 1:2 (1 becomes 2)
    # User Example: Price 100 -> 50.
    res = CorporateActionEngine.calculate_split("1:2")
    print(f"SPLIT 1:2 (Old:New): {res}")
    # Verify: Price Multiplier should be 0.5
    assert res['price_multiplier'] == 0.5
    print("OK - Split Verified\n")

    # 2. Bonus 1:1
    # User Example: Price 200 -> 100.
    res = CorporateActionEngine.calculate_bonus("1:1")
    print(f"BONUS 1:1 (Bonus:Held): {res}")
    # Verify: Price Multiplier should be 0.5
    assert res['price_multiplier'] == 0.5
    print("OK - Bonus Verified\n")

    # 3. Rights 3:25 @ 1799 (Using Adani Example, but need P)
    # User specific example logic: Rights 1:14 @ 530, P=735.
    # Expect Multiplier 0.9814
    res = CorporateActionEngine.calculate_rights("1:14", 530, 735)
    print(f"RIGHTS 1:14 @ 530 (P=735): {res}")
    
    expected_mult = 0.9814
    diff = abs(res['price_multiplier'] - expected_mult)
    if diff < 0.0001:
        print(f"OK - Rights Verified (Calculated {res['price_multiplier']:.4f} matches {expected_mult})")
    else:
        print(f"FAIL - Rights Mismatch! Expected {expected_mult}, Got {res['price_multiplier']:.4f}")
    print("\n")

    # 4. Demerger
    res = CorporateActionEngine.calculate_demerger("1:1", cost_split_parent_pct=70)
    print(f"DEMERGER: {res}")
    assert res['price_multiplier'] == 1.0
    print("OK - Demerger Verified")
