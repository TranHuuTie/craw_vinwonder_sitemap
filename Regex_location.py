import re
import pandas as pd
import unicodedata

import pandas as pd

# Đọc file pickle
file_path = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_.pkl"
df = pd.read_pickle(file_path)
# Đánh số lại cột ID từ 1 đến tổng số dòng
df["ID"] = range(1, len(df) + 1)


# Chuẩn hóa tiếng Việt: Bỏ dấu, chuyển thành chữ thường
def remove_accents(text):
    if not isinstance(text, str):
        return ""
    nfkd_form = unicodedata.normalize('NFKD', text)
    return ''.join([c for c in nfkd_form if not unicodedata.combining(c)]).casefold().strip()

# Chuẩn hóa từ điển: Bỏ dấu toàn bộ key
def normalize_dict_keys(mapping):
    return {remove_accents(key): value for key, value in mapping.items()}
# # Chuẩn hóa dictionary về chữ thường
# def normalize_dict_keys(mapping):
#     return {key.casefold(): value for key, value in mapping.items()}

# Chuẩn hóa dữ liệu ánh xạ về chữ thường
location_mapping = normalize_dict_keys({
    "Hanoi": "ha-noi","Ha Noi": "ha-noi","Hà Nội": "ha-noi","thủ đô": "ha-noi","timecity":"ha-noi","hung-yen":"ha-noi","hn-":"ha-noi","Hung Yen": "ha-noi", #Tiếng Anh - Tiếng Việt
    "Phu Quoc": "phu-quoc","Phú Quốc": "phu-quoc","Phuquoc": "phu-quoc", 
    "Vinhomes Ocean Park 3": "ha-noi","Vinhomes Ocean Park 1": "ha-noi", "Vinhomes Ocean Park 2": "ha-noi",
    "Nghe An": "cua-hoi","Nghệ An": "cua-hoi","Nghean": "cua-hoi", "Cửa Hội":"cua-hoi", "Cua Hoi":"cua-hoi","nghe-an":"cua-hoi",
    "Hoi An": "nam-hoi-an","Hội An": "nam-hoi-an","Nam Hội An": "nam-hoi-an",
    "Quảng Nam": "nam-hoi-an","Quang Nam": "nam-hoi-an","Quangnam": "nam-hoi-an","nam-hoi-an":"nam-hoi-an","Huế": "nam-hoi-an", "Hue": "nam-hoi-an",
    "Kien Giang": "phu-quoc","Kiengiang": "phu-quoc","Kiên Giang": "phu-quoc","kien-giang":"phu-quoc",
    "Da Nang":"nam-hoi-an","Danang": "nam-hoi-an","Đà Nẵng": "nam-hoi-an",
    "Halong": "ha-long","ha-long": "ha-long","Hạ Long": "ha-long","Hai Phong":"ha-long",
    "Quang Ninh": "ha-long","Quangninh": "ha-long","quang-ninh":"ha-long","hai-phong":"ha-long",
    "Nha Trang": "nha-trang","Nhatrang": "nha-trang","Hon Tam": "nha-trang", "Hòn Tằm": "nha-trang","aquafield":"nha-trang","hon-tam":"nha-trang",
    "Phú yên": "phu-yen","ninh-binh": "ninh-binh","Ninhbinh": "ninh-binh",
    "Ho Chi Minh": "ho-chi-minh","Hồ Chí Minh": "ho-chi-minh","Sài Gòn": "ho-chi-minh","Sai Gon": "ho-chi-minh",
    "Đà Lạt": "da-lat","Da Lat": "da-lat","Dalat":"da-lat",
    "Hà Giang": "ha-giang","Ha Giang": "ha-giang",
    "Tây Ninh": "tay-ninh","Tay Ninh": "tay-ninh",
    "Sapa": "Lao Cai","quy-nhon": "quy-nhon","Vung Tau":"vung-tau",
    "Vọng Hải giải Bạch Mã": "nam-hoi-an", "Thanh Hóa":"thanh-hoa",
    "Quang Binh": "quang-binh","Quảng Bình": "quang-binh",

    "하노이": "ha-noi","호아빈": "hoa-binh","소나": "son-la","디엔 비엔": "dien-bien","라이차우": "lai-chau","라오까이": "lao-cai","옌베이": "yen-bai","푸토": "phu-tho","하장": "ha-giang",
    "뚜옌꽝": "tuyen-quang","카아오방": "cao-bang","박깐": "bac-kan","타이응우옌": "thai-nguyen","랑손": "lang-son","바짱": "Bắc Giang","꽝닌": "Quảng Ninh","박닌": "bac-ninh","하남": "ha-nam",
    "하이두옹": "Hải Dương","하이퐁": "ha-long","흥옌": "ha-noi","남딘": "nam-dinh","타이빈": "thai-binh","빈푹": "vinh-phuc","닌빈": "ninh-binh","타니호아": "thanh-hoa","응에안": "cua-hoi",
    "하띤": "cua-hoi", "꽝빈": "quang-binh","쾅트리": "Quang-tri","투안티엔후에": "nam-hoi-an", "다낭": "nam-hoi-an", "호치민": "HCM", "호치민시":"HCM", "사이공": "HCM",
    "꽝남": "nam-hoi-an", "꽝응아이": "quang-ngai", "빈딘": "binh-dinh", "푸옌": "phu-yen","칸호아": "nha-trang", "닌 투언": "ninh-thuan", "빈 투언": "binh-thuan", "꼰뚬": "Kon-Tum",
    "기아 라이": "gia-lai","닥락": "Dak-Lak","닥농": "Dak-Nong","램동": "lam-dong","빈 푸우크": "binh-phuoc","동나이": "dong-nai","바리아 붕타우": "vung-tau","테이 닌": "tay-ninh","빈둥": "binh-duong",
    "롱안": "long-an","티엔쟝": "tien-giang","벤째": "ben-tre","동탑 성": "dong-thap","빈롱": "vinh-long","트라빈": "tra-vinh","안장": "an-giang",
    "껀터": "can-tho","하우장": "hau-giang", "속짱": "soc-trang","끼엔장": "phu-quoc","박리우": "bac-lieu","까마우": "ca-mau",

    "빈펄 사파리 푸꾸옥":"phu-quoc", "빈펄 사파리 푸꾸옥행 버스":"phu-quoc", "빈원더스 푸꾸옥행 버스":"phu-quoc", " 푸꾸옥까지":"phu-quoc", "빈펄 나트랑":"nha-trang",
    "나트랑":"nha-trang","칸호아":"nha-trang", "Hon Tre":"nha-trang","혼탐":"nha-trang",
    "푸꾸옥":"phu-quoc", " 푸꾸옥의":"phu-quoc","끼엔장":"phu-quoc", 
    "남호이안":"nam-hoi-an","호이안":"nam-hoi-an","다낭":"nam-hoi-an", "광남":"nam-hoi-an","후에":"nam-hoi-an",
    "하롱":"ha-long", "꽝닌":"ha-long", 
    "하롱베이":"ha-long"

    # "빈펄 사파리 푸꾸옥":"phu-quoc", "빈펄 사파리 푸꾸옥행 버스":"phu-quoc", "빈원더스 푸꾸옥행 버스":"phu-quoc", # Tiếng Hàn
    # "나트랑":"nha-trang","칸호아":"Khánh Hòa", "Khánh Hòa":"Hòn Tre","혼탐":"Hòn Tằm",
    # "푸꾸옥":"phu-quoc", "끼엔장":"phu-quoc", 
    # "남호이안":"nam-hoi-an","호이안":"nam-hoi-an","다낭":"Da Nang", "광남":"Quang Nam","후에":"nam-hoi-an",
    # "하롱":"Ha Long", "꽝닌":"Quang Ninh",

})

facility_mapping = normalize_dict_keys({
    "vinpearl golf nha trang": "Vinpearl Golf Nha Trang", "Ban Do Am Thuc Vinpearl Hon Tre":"Ban Do Am Thuc Vinpearl Hon Tre",# vinpearl
    "Hon Tam Resort":"Hon Tam Resort", "Hon Tam": "Hon Tam Resort",
    "vinpearl luxury nha trang":"Vinpearl Luxury Nha Trang", "resort vinpearl nha trang":"Resort Vinpearl Nha Trang",
    "vinpearl resort nha trang":"Vinpearl Resort Nha Trang","Vinpearl Beachfront Nha Trang":"Vinpearl Beachfront Nha Trang",
    "resort & spa nha trang bay":"Vinpearl Resort & Spa Nha Trang Bay","vinpearl nha trang hon tre":"Vinpearl Nha Trang Hon Tre", "vinpearl nha trang":"Vinpearl Nha Trang",
    "Vinpearl Resort & Spa Long Beach Nha Trang":"Vinpearl Resort & Spa Long Beach Nha Trang",

    "Vinpearl Wonderworld Phu Quoc":"Vinpearl Wonderworld Phu Quoc","VinHolidays Fiesta Phu Quoc":"VinHolidays Fiesta Phu Quoc", "Vinpearl Resort & Spa Phu Quoc":"Vinpearl Resort & Spa Phu Quoc",
    "vinpearl safari phu quoc": "Vinpearl Safari Phu Quoc", "vinpearl safari in phu quoc": "Vinpearl Safari Phu Quoc",
    "Vinpearl Golf Phu Quoc":"Vinpearl Golf Phu Quoc","Grand World Phu Quoc":"Grand World Phu Quoc",
    "vinpearl phu quoc": "Vinpearl Phu Quoc", "Vinpearl Resort Phú Quốc": "Vinpearl Resort Phu Quoc", "Vinpearl Resort Phu Quoc":"Vinpearl Resort Phu Quoc",

    "vinpearl resort & spa da nang":"Vinpearl Resort & Spa Da Nang","Vinpearl Resort & Spa Đà Nẵng": "Vinpearl Resort & Spa Da Nang", "vinpearl resort da nang": "Vinpearl Resort Da Nang", 
    "vinhomes ocean park": "Vinhomes Ocean Park", "Grand World Hung Yen":"Vinhomes Ocean Park 2 3","Grand World Ha Noi":"Vinhomes Ocean Park 2 3" ,"Grand World: A new entertainment complex near Hanoi":"Vinhomes Ocean Park 2 3",
    "vinhomes grand park": "Vinhomes Grand Park", "Stayndfun Homestay":"Stayndfun Homestay", "StaynFun":"Vinpearl HN",
    "Vinpearl Resort & Golf Nam Hoi An":"Vinpearl Resort & Golf Nam Hoi An","Vinpearl Resort & Spa Hoi An":"Vinpearl Resort & Spa Hoi An","vinpearl nam hoi an":"Vinpearl Nam Hoi An",
    "Vinpearl Golf Nam Hoi An":"Vinpearl Golf Nam Hoi An", "Safari Nam Hoi An":"Vinpearl River Safari Nam Hoi An",
    "vinpearl resort & spa ha long":"Vinpearl Resort & Spa Ha Long", 
    "Vinpearl Golf Hai Phong":"Vinpearl Golf Hai Phong", "Vinpearl Horse Academy":"Vinpearl Horse Academy","Vinpearl Safari":"Vinpearl Safari",
     #HN
#vinwonder
    "Grand World":"Grand World","Vinke $ Vinpearl Aquarium":"Vinke $ Vinpearl Aquarium","VinWonders Ware Park & Water Park":"VinWonders Ware Park & Water Park",

    "vinwonders nha trang": "VinWonders Nha Trang","VinWonders Nha Trang":"VinWonders Nha Trang","Aquafield Nha Trang":"Aquafield Nha Trang","(Aquafield)":"Aquafield Nha Trang",
    "Hon Tam Mud Bath":"Hon Tam Mud Bath","Vinpearl Harbour Nha Trang":"Vinpearl Harbour Nha Trang","Vinpearl Harbour":"Vinpearl Harbour Nha Trang","(Vinpearl Resort Nha Trang)":"Vinpearl Resort Nha Trang",
    
    "Vinwonders Phu Quoc":"Vinwonders Phu Quoc","Grand World Phu Quoc":"Grand World Phu Quoc","Vinpearl Safari Phu Quoc":"Vinpearl Safari Phu Quoc",
    "và Safari Phú Quốc":"Vinpearl Safari Phu Quoc","VinWonders và Safari Phú Quốc":"VinWonders Phu Quoc",
    "Vinwonders Nam Hoi An":"Vinwonders Nam Hoi An",  "Vinwonders Cua Hoi":"Vinwonders Cua Hoi", #NA 

    "VinWonders Ha Tinh":"VinWonders Ha Tinh", 

    "Road to 8Wonder -The next icon":"Road to 8Wonder -The next icon","Vinpearl Horse Academy Vu Yen":"Vinpearl Horse Academy Vu Yen",

    "Grand Park Amusement Park":"Grand Park Amusement Park",

    "VinWonders Wave Park & Water Park":"VinWonders Wave Park & Water Park","VinWonders Wave Park":"VinWonders Wave Park",


    "빈펄 사파리 푸꾸옥":"Vinpearl Safari Phu Quoc","빈펄 사파리 푸꾸옥행 버스":"Vinpearl Safari Phu Quoc", "빈원더스 푸꾸옥행 버스":"VinWonders Phu Quoc" ,  # Tiếng Hàn
    "빈홀리데이즈 피에스타 푸꾸옥":"Vinholidays Fiesta Phu Quoc","빈펄 원더월드 푸꾸옥":"Vinpearl Wonderworld Phu Quoc",
    "빈펄 푸꾸옥":"Vinpearl Phu Quoc","빈펄 푸꾸옥의":"Vinpearl Phu Quoc", "빈펄 리조트 & 스파 푸꾸옥":"Vinpearl Resort & Spa Phu Quoc", 
    
    "혼땀 리조트":"Hon Tam Resort","빈펄 나트랑":"Vinpearl Nha Trang","빈펄 나트랑과":"Vinpearl Nha Trang","빈펄 리조트 & 스파 나트랑 베이":"Vinpearl Resort & Spa Nha Trang Bay",
    "빈펄 리조트 나트랑":"Vinpearl Nha Trang","빈펄 럭셔리 나트랑":"Vinpearl Luxury Nha Trang","빈펄 럭셔리 나트랑의":"Vinpearl Luxury Nha Trang","빈펄 비치프론트 나트랑":"Vinpearl Beachfront Nha Trang",
    "나트랑 혼트레 섬 빈펄 (Hon Tre Island)":"VInpearl Nha Trang Hon Tre",

    "빈펄 남호이안":"Vinpearl Nam Hoi An", "빈펄 리조트 & 골프 남호이안":"Vinpearl Resort & Golf Nam Hoi An",

    "빈펄 하롱":"Vinpearl Ha Long","빈펄 리조트 & 스파 하롱":"Vinpearl Resort & Spa Ha Long"

    
    })

# Tạo regex pattern (đã chuẩn hóa key)
def create_regex_pattern(mapping):
    return r'\b(' + '|'.join(re.escape(key) for key in mapping.keys()) + r')\b'

location_pattern = create_regex_pattern(location_mapping)
facility_pattern = create_regex_pattern(facility_mapping)

# # Hàm trích xuất thông tin
def extract_info(text, pattern, mapping):
    text = remove_accents(text)  # Chuẩn hóa đầu vào
    if not text:
        return []
    matches = re.findall(pattern, text, re.IGNORECASE)
    standardized_matches = {mapping.get(match, match) for match in matches}
    return list(standardized_matches)  # Trả về danh sách thay vì chuỗi

# Áp dụng tìm kiếm trên TITLE, ABSTRACT
df['Location'] = df.apply(lambda row: extract_info(f"{row['TITLE']} {row['ABSTRACT']}", location_pattern, location_mapping), axis=1)
df['Facility'] = df.apply(lambda row: extract_info(f"{row['TITLE']} {row['ABSTRACT']}", facility_pattern, facility_mapping), axis=1)

df["Location"] = df["Location"].apply(lambda x: [item.lower().replace(" ", "-") for item in x] if isinstance(x, list) and x else [])
df["Facility"] = df["Facility"].apply(lambda x: [item.lower().replace(" ", "-") for item in x] if isinstance(x, list) and x else [])

# Lưu lại file sau khi biến đổi
output_file_path = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_modified.pkl"
df.to_pickle(output_file_path)