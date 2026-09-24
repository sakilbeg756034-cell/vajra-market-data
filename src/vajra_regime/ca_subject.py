r"""ca_subject.py -- NSE corporate action ki EK line se bhaav ka factor. EKMATRA JAGAH.

25-Sep-2026 tak ye tark DO jagah tha aur dono alag the:

  * laptop (`D:\VAJRA_RESEARCH\work\scripts\ca_factor.py`) -- sudhara hua
  * cloud (`corporate_actions.classify_adjustment`) -- purana, teen kamiyon ke saath:
    DIVIDEND ko BONUS se pehle dekhna ("Bonus 2:1/Dividend" -> sirf dividend),
    BONUS ko SPLIT se pehle dekhna ("Bonus 1:1 And Face Value Split" -> sirf 0.5),
    aur "Fv Splt Frm Rs 10 To Re 1" jaise chhote likhe split na pehchaanna.

Yaani laptop ka backtest aur live signal ek hi event ko alag-alag samajh sakte
the (VAJRA jaal #3: "ek cheez do jagah likhi ho to wo alag ho jaati hai").
Ab dono yahi module bulate hain:

  * laptop: `ca_factor.py` (wrapper) -> `build_adjusted_panel.py`, `publish_vajra_data.py`
  * cloud:  `cloud/daily.py`, `cloud/bootstrap.py`

Koi side-effect nahi (na stdout, na file, na network). `classify_adjustment`
(purana) abhi bhi `ca_repair.py` aur engine ke store ke liye rehta hai; bonus/
split ke bahar ki kism (dividend, buyback...) ke liye yahan bhi wahi bulaya jaata hai.

DEMERGER (25-Sep-2026): subject se factor nahi nikalta, isliye EX-DATE ke bhaav
se -- `demerger_price_factor()`. Pehle demerger ka jhatka adjust nahi hota tha:
jo share pehle se pakde the unhe nakli giraav lagta (STAR 2024-12-06 -54%) aur
R12 saal bhar toota rehta (cloud ki 24-Sep file me VEDL R12 -42%, vol 111%).
"""
from __future__ import annotations

import re

BONUS_RE = re.compile(r"\bBON(?:US)?\b[^0-9]*(\d+(?:\.\d+)?)\s*:\s*(\d+(?:\.\d+)?)")
SPLIT_RE = re.compile(r"FROM[^0-9]*(\d+(?:\.\d+)?).*?TO[^0-9]*(\d+(?:\.\d+)?)")
# NSE ke CHHOTE likhe hue split -- "FROM" ke bina (24-Sep-2026 ko joda gaya).
#
#   "Fv Splt Frm Rs 10 To Re 1"      JSWSTEEL 2017-01-04 (+13 aur, 2016-17)
#   "Fv Spl-Rs10tors5/Bon-2:1"       ENGINERSIN 2010-05-06
#   "Bon 1:1/Fv Spl Rs.5tors.2"      STLTECH 2010-03-09
#
# Pehle ye "SPLIT" shabd na hone se OTHER ban kar CHUP-CHAAP chhoot jaate the:
# na adjust, na quarantine. JSWSTEEL ka us din ka return -90% reh gaya (asal
# me +1.4%) -- aur V2.1 us din JSWSTEEL pakde hue tha (backtest me -88.6%
# ka nakli nuksaan). Poori list: VAJRA_INTRADAY KNOWN_ISSUES K-36.
FV_SPLIT_RE = re.compile(
    r"(?:RS|RE)\.?\s*(\d+(?:\.\d+)?)[^0-9]*?TO\s*(?:RS|RE)?\.?\s*(\d+(?:\.\d+)?)")
# "SPL" akela SPECIAL dividend bhi hota hai ("Div-Fin Rs.5+Spl Rs.10") -- isliye
# chhota roop tabhi split maana jaata hai jab saath me FV ho ya SPLT likha ho.
SPLIT_WORDS = ("SPLIT", "SUB-DIVISION", "SUB DIVISION", "SPLT", "FV SPL")
BLOCKERS = ("DEMERGER", "DE-MERGER", "MERGER", "AMALGAM", "RIGHT",
            # Capital reduction me share ginti ka anupaat subject se saaf nahi
            # nikalta (MONNETISPA 2018: "Capital Reduction Rs 10 To Rs 3.30 /
            # Consolidation Rs 3.30 To Rs.10") -- haath se dekhna.
            "CAPITAL REDUCTION")

# Bonus hamesha EQUITY share ka nahi hota. Company preference share, NCRPS ya
# debenture bhi bonus me baant sakti hai -- aur usme equity share TOOTA NAHI
# hota, sirf ek alag cheez muft milti hai.
#
# Pakda gaya (naye panel ki purane se tulna me):
#
#   ZEEL       2014-03-03  "Bonus Preference Shares 21:1"
#              -> equity ka formula 1/22 deta hai, itihaas 22 se bhaag ho gaya,
#                 aur us din ka return +2,088% ho gaya (asal me -0.52%)
#   NTPC       2015-03-20  "Scheme Of Arrangement - Bonus Debentures 1:1"
#   TVSMOTOR   2025-08-25  "Scheme Of Arrangement - Bonus Ncrps 4:1"
#   COROMANDEL 2012-07-13  "... Scheme Of Arrangement - Bonus Debentures ..."
#
# Aise event me bhaav thoda girta zaroor hai (jo cheez baanti gayi uski keemat
# nikal jaati hai), par kitna -- ye subject padh kar nikaalna mumkin nahi.
# Isliye adjust NAHI kiya jaata, aur bacha hua jhatka residual_shocks me chhap
# jaata hai. Aadha adjustment bilkul na karne se zyada khatarnak hota hai.
NON_EQUITY = ("PREFERENCE", "NCRPS", "OCRPS", "CRPS", "DEBENTURE", "NCD",
              "WARRANT", "BOND",
              # DVR ek ALAG share hai (JISLJALEQS 2011: "Bon 1 Dvr : 20 Eq
              # Shares") -- ordinary share toota nahi, sirf alag cheez mili.
              "DVR")


def price_factor(subject: str) -> tuple[float | None, str, str]:
    """Ek CA subject ka bhaav-factor.

    Wapas: (factor, kism, wajah). factor None = "khud se adjust mat karo".

    ENGINE KE PARSER ME DO GALTIYAN THIN, DONO EK HI KISM KI
    --------------------------------------------------------
    `classify_adjustment()` subject me shabd ek KRAM se dhoondhta hai aur pehla
    match milte hi laut jaata hai. Bharat me ek hi CA line me kai cheezein hoti
    hain, isliye ye kram do jagah chup-chaap galat jawab deta hai:

      1. DIVIDEND, BONUS se PEHLE aata hai. Isliye:

             "Bonus 2:1/Dividend- Rs 1.60 Per Share"   ->  sirf DIVIDEND

         Pakda gaya: UNOMINDA, 11-Jul-2018. Bonus 2:1 ka factor 1/3 hai, par
         event "dividend" maan liya gaya, adjustment laga hi nahi, aur us din
         ka return -66.4% reh gaya (asal me +0.67%).

      2. BONUS, SPLIT se PEHLE aata hai. Isliye:

             "Bonus 1:1 And Face Value Split From Rs.10 To Re.1"  ->  sirf 0.5

         jabki asli factor 0.5 x 0.1 = 0.05 hai. Feed me aise 31 event hain.

    Dono galtiyan ek hi wajah se hain: "pehla match jeet gaya". Isliye yahan
    kram nahi, GINTI chalti hai -- jitni cheezein bhaav badalti hain, sabka
    factor nikaal kar guna kiya jaata hai.

    Ek cheez pehle rukti hai: demerger, merger aur rights. Unka factor subject
    padh kar nikaalna mumkin hi nahi, aur unke saath bonus/split laga dena aadha
    adjustment hoga -- jo bilkul na karne se zyada khatarnak hai.
    """
    up = str(subject).upper().replace("\u2013", "-").replace("\u2014", "-")

    if any(b in up for b in BLOCKERS):
        return None, "REVIEW_NEEDED", "demerger/merger/rights -- haath se dekhna hoga"

    factor, parts = 1.0, []

    if re.search(r"\bBON(?:US)?\b", up):
        if any(w in up for w in NON_EQUITY):
            return None, "BONUS_GAIR_EQUITY", \
                "equity ka bonus nahi (preference/NCRPS/debenture) -- haath se dekhna hoga"
        m = BONUS_RE.search(up)
        if not m:
            return None, "BONUS", "bonus ka ratio nahi padha ja saka"
        new, old = float(m.group(1)), float(m.group(2))
        if new <= 0 or old <= 0:
            return None, "BONUS", "bonus ka ratio galat hai"
        factor *= old / (old + new)
        parts.append(f"bonus {new:g}:{old:g}")

    if "CONSOLIDAT" in up:
        # Ulta split: 10 purane share -> 1 naya, bhaav 10 guna. Pehle ye
        # chup-chaap chhoot jaata tha aur ex-date par NAKLI tezi banti thi
        # (VERTOZ 2025-06-25: "Consolidation ... From Re 1 ... To Rs 10" ->
        # +900%). Factor > 1: purane bhaav 10 se guna.
        m = SPLIT_RE.search(up) or FV_SPLIT_RE.search(up)
        if not m:
            return None, "CONSOLIDATION", "consolidation ka anupaat nahi padha ja saka"
        of, nf = float(m.group(1)), float(m.group(2))
        if of <= 0 or nf <= of:
            return None, "CONSOLIDATION", "consolidation ka anupaat galat hai"
        factor *= nf / of
        parts.append(f"consolidation {of:g}->{nf:g}")
    elif any(w in up for w in SPLIT_WORDS):
        m = SPLIT_RE.search(up) or FV_SPLIT_RE.search(up)
        if not m:
            return None, "SPLIT", "face value nahi padhi ja saki"
        of, nf = float(m.group(1)), float(m.group(2))
        if of <= 0 or nf <= 0 or nf >= of:
            # nf >= of ka matlab consolidation hai (share ghat-te hain), aur
            # uska hisaab ulta chalta hai -- alag se dekhna behtar hai.
            return None, "SPLIT", "face value galat ya consolidation lagta hai"
        factor *= nf / of
        parts.append(f"face {of:g}->{nf:g}")

    if parts:
        kind = "BONUS+SPLIT" if len(parts) > 1 else parts[0].split()[0].upper()
        return factor, kind, " x ".join(parts)

    # Na bonus, na split -- baaki sab (dividend, buyback wagairah) bhaav ka
    # paimana nahi badalte. Engine ka parser inhe theek batata hai.
    from vajra_regime.corporate_actions import classify_adjustment  # noqa: PLC0415
    p = classify_adjustment(str(subject))
    return None, p.action_type, p.note


# ---------------------------------------------------------------- DEMERGER
#
# Factor = ex-date ka OPEN / pichhle session ka CLOSE (dono NSE ke KACHCHE
# bhaav). NSE khud ex-date par special pre-open se yahi bhaav dhoondhta hai.
# Ye ANUMAAN hai (us subah bazaar ki chaal bhi isme hai), isliye sakht shartein:
#   * "DEMERGER" shabd wala event: bhaav 3% se zyada gira ho
#   * "Scheme Of Arrangement" (bina merger/amalgamation): 25% se zyada gira ho
#     -- warna wo merger ya kuch aur bhi ho sakta hai
#   * bhaav 98% se zyada gira ho to data par shak -- factor nahi
DEMERGER_MAX_RATIO = 0.97
SCHEME_MAX_RATIO = 0.75
MIN_RATIO = 0.02

# Wo kism jinka factor kabhi-kabhi nahi nikalta ("anupaat-heen") -- cloud ka
# quarantine niyam 1 inhe dekhta hai (purane naam bhi, purani state file ke liye).
UNRATIOED_KINDS = ("RIGHTS", "MERGER", "DEMERGER", "SPLIT", "BONUS",
                   "REVIEW_NEEDED", "BONUS_GAIR_EQUITY", "CONSOLIDATION")


def demerger_kind(subject: str) -> str | None:
    """'DEMERGER', 'SCHEME' ya None -- sirf subject se."""
    up = str(subject).upper()
    if "RIGHT" in up:
        return None
    if "DEMERGER" in up or "DE-MERGER" in up:
        return "DEMERGER"
    if "SCHEME OF ARRANGEMENT" in up and "AMALGAM" not in up and "MERGER" not in up:
        return "SCHEME"
    return None


def demerger_price_factor(subject: str, open_ex: float, close_prev: float) -> float | None:
    """Demerger ka bhaav-factor, ya None (lagao mat)."""
    kind = demerger_kind(subject)
    if kind is None:
        return None
    try:
        op, pc = float(open_ex), float(close_prev)
    except (TypeError, ValueError):
        return None
    if not (op > 0 and pc > 0):
        return None
    ratio = op / pc
    limit = SCHEME_MAX_RATIO if kind == "SCHEME" else DEMERGER_MAX_RATIO
    if MIN_RATIO < ratio < limit:
        return ratio
    return None


# Jo kism bhaav ka paimana badalti hi nahi -- factor 1.0 (cloud ka purana
# `classify_adjustment` bhi inhe 1.0 deta tha; state file ka yahi rivaaz hai).
NO_ADJUST_KINDS = ("DIVIDEND", "BUYBACK")


def event_columns(subjects) -> dict[str, list]:
    """Cloud ke event table ke chaar column, subject ki list se -- ek jagah.

    `cloud/daily.py` aur `cloud/bootstrap.py` dono yahi bulate hain.
    """
    price, volume, kind, status = [], [], [], []
    for subject in subjects:
        f, k, _ = price_factor(str(subject))
        if f is None and k in NO_ADJUST_KINDS:
            f = 1.0
        price.append(f)
        volume.append(None if f is None else 1.0 / f)
        kind.append(k)
        status.append("PARSED" if f is not None else "REVIEW")
    return {"PriceFactor": price, "VolumeFactor": volume, "ActionType": kind,
            "ParseStatus": status}
