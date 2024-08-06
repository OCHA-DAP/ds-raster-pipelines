def to_leadtime(start_month, end_month):
    """
    Given an input starting month and ending month,
    gives the number of months leadtime between start and end.

    Args:
        start_month (int): Starting month
        end_month (int): Ending month

    Returns:
        (str): Number of months leadtime between start and end
    """
    if end_month < start_month:
        end_month += 12
    return end_month - start_month


def leadtime_months(start_month, leadtime):
    """
    Given an input starting month and leadtime,
    gives a list of all months across the leadtime.

    Args:
        start_month (int): Starting month
        leadtime (int): Number of months leadtime

    Returns:
        (list): List of all months across the leadtime
    """
    return [(start_month + i - 1) % 12 + 1 for i in range(leadtime)]
