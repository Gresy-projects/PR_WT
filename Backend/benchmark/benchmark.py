def calculate_metrics(exact_val, approx_val, exact_time_ms, approx_time_ms):
    """
    Takes the results and times from both engines and calculates the trade-offs.
    """
    # Ensure values are floats for math operations
    try:
        e_val = float(exact_val)
        a_val = float(approx_val)
    except (TypeError, ValueError):
        return {"error": "Cannot calculate metrics on non-numeric or grouped data yet."}

    # Calculate the Error Percentage
    if e_val != 0:
        error_pct = abs(e_val - a_val) / e_val * 100
    else:
        error_pct = 0.0

    accuracy_pct = 100.0 - error_pct
    
    # Calculate the Speedup Multiplier
    if approx_time_ms > 0:
        speedup = exact_time_ms / approx_time_ms
    else:
        speedup = 0.0 

    return {
        "exact_value": round(e_val, 2),
        "approx_value": round(a_val, 2),
        "exact_time_ms": round(exact_time_ms, 4),
        "approx_time_ms": round(approx_time_ms, 4),
        "error_percentage": round(error_pct, 4),
        "accuracy_percentage": round(accuracy_pct, 2),
        "speedup_x": round(speedup, 2),
        "met_3x_target": speedup >= 3.0  
    }