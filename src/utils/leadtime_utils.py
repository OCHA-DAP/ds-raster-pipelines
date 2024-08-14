def to_leadtime(pub_month, fc_month):
    """
    Given an input publication month and forecast month,
    gives the number of months leadtime between publication and forecast.

    Parameters:
        pub_month (int): Month the forecast was published
        fc_month (int): Month the forecast applies to

    Returns:
        (str): Number of months leadtime between start and end
    """
    if fc_month < pub_month:
        fc_month += 12
    return fc_month - pub_month


def leadtime_months(start_month, leadtime):
    """
    Given an input starting month and leadtime,
    gives a list of all months across the leadtime.

    Parameters:
        start_month (int): Starting month
        leadtime (int): Number of months leadtime

    Returns:
        (list): List of all months across the leadtime
    """
    return [(start_month + i - 1) % 12 + 1 for i in range(leadtime)]
