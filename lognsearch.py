# Functions to search country code with iplocations
def binarySearch(target_ip, ip_locations):
    low = 0
    high = len(ip_locations) - 1
    iterations = 0
    Country = "NA"
    while low <= high:
        middle = int((low + high) / 2)
        # check if we found it
        if target_ip >= ip_locations.IPmin[middle] and target_ip <= ip_locations.IPmax[middle]:
            Country = ip_locations.Countrycode[middle]
            break
        # check which side
        elif target_ip < ip_locations.IPmin[middle]:
            iterations += 1
            high = middle - 1
        else:
            iterations += 1
            low = middle + 1
    return (Country)