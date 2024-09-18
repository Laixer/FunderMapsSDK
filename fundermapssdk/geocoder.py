def is_bag_id(s: str) -> bool:
    if s.startswith("NL.IMBAG.PAND."):
        return True
    if len(s) == 16:
        return s.isdigit()
