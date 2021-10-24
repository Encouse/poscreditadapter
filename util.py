
def rgetattr(obj, attr_list):
    if not attr_list:
        return obj
    obj = getattr(obj, attr_list.pop(0))
    return rgetattr(obj, attr_list)


def check_ip(ip, mask, hostsnum):
    octets = ip.split('.')
    mask = mask.split('.')
    if mask[0] == octets[0] and mask[1] == octets[1]:
        return hostsnum >= int(octets[2]) - mask[3] * int(octets[3]) >= 0
    return False
