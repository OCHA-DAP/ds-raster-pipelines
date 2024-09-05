def to_leadtime(pub_month, fc_month):
    """
    Given an input publication month and forecast month,
    gives the number of months leadtime between publication and forecast.

    Parameters:
        pub_month (int): Month the forecast was published
        fc_month (int): Month the forecast applies to

    Returns:
        (int): Number of months leadtime between start and end
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


def to_fc_month(pub_month, leadtime):
    """
    Given an input publication month and leadtime,
    gives the forecast month. Note that leadtimes index from 0,
    so an input `pub_month` of 1 and leadtime of 0 (for example)
    will return 1.

    Parameters:
        pub_month (int): Month the forecast was published
        leadtime (int): Number of months leadtime

    Returns:
        (int): Month the forecast applies to
    """
    return (pub_month + leadtime - 1) % 12 + 1


def to_fc_year(pub_month, pub_year, leadtime):
    """
    Given an input publication month, publication year,
    and leadtime, gives the forecast year.

    Parameters:
        pub_month (int): Month the forecast was published
        pub_year (int): Year the forecast was published
        leadtime (int): Number of months leadtime

    Returns:
        (int): Year the forecast applies to
    """
    years_to_add = (pub_month + leadtime - 1) // 12
    return pub_year + years_to_add
