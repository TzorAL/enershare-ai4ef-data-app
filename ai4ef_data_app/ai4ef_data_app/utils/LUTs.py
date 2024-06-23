# Url format is https://videscentrs.lvgmc.lv/noverojumu-arhivs/meteo/{station_code}

# Station codes extracted from here: https://videscentrs.lvgmc.lv/index.89b4a874.js
class Stations:
    def __init__(self):
        self.Ainazi = "30000"
        self.Aluksne = "30004"
        self.Bauska = "30011"
        self.Daugavpils = "30021"
        self.Dobele = "30022"
        self.Gulbene = "30034"
        self.Jelgava = "30036"
        self.Kolka = "30046"
        self.Liepāja = "30060"
        self.Mersrags = "30072"
        self.Pavilosta = "30080"
        self.Priekuļi = "30087"
        self.Riga_Universitate = "30096"
        self.Rucava = "30099"
        self.Rujiena = "30100"
        self.Saldus = "30102"
        self.Skrīveri = "30105"
        self.Skulte = "30106"
        self.Stende = "30111"
        self.Ventspils = "30128"
        self.Zilani = "30140"
        self.Zoseni = "30141"
        self.Dagda = "10000120"
        self.Rezekne = "10000180"

class Meteorology:
    def __init__(self):
        self.aramkartas_temp_10cm = '4514'  # Temperature of the plow layer at 10 cm depth, actual, °C
        self.aramkartas_temp_15cm_1 = '4515'  # Temperature of the plow layer at 15 cm depth, actual, °C
        self.aramkartas_temp_15cm_2 = '4516'  # Temperature of the plow layer at 15 cm depth, actual, °C
        self.aramkartas_temp_5cm = '4513'  # Temperature of the plow layer at 5 cm depth, actual, °C
        self.atmosferas_spiediens = '4167'  # Atmospheric pressure at station level, actual, hPa
        self.gaisa_temp_faktiska = '4001'  # Air temperature, actual, °C
        self.gaisa_temp_max_3st = '4008'  # Air temperature, maximum in the last 3 hours, °C
        self.gaisa_temp_min_3st = '4003'  # Air temperature, minimum in the last 3 hours, °C
        self.gaisa_temp_stundas_max = '4006'  # Air temperature, hourly maximum, °C
        self.gaisa_temp_stundas_min = '4004'  # Air temperature, hourly minimum, °C
        self.gaisa_temp_stundas_videja = '4002'  # Air temperature, hourly average, °C
        self.makonu_augstums_1 = '10193'  # Cloud height 1, m
        self.makonu_augstums_2 = '10194'  # Cloud height 2, m
        self.makonu_augstums_3 = '10195'  # Cloud height 3, m
        self.makonu_daudums_1 = '10196'  # Cloud amount 1, Okta
        self.makonu_daudums_2 = '10197'  # Cloud amount 2, Okta
        self.makonu_daudums_3 = '10198'  # Cloud amount 3, Okta
        self.redzamiba_faktiska = '9954'  # Meteorological visibility, actual, m
        self.redzamiba_stundas_max = '4674'  # Meteorological visibility, hourly maximum, m
        self.redzamiba_stundas_min = '4672'  # Meteorological visibility, hourly minimum, m
        self.nokrisnu_daudums_stundas_summa = '4570'  # Precipitation amount, hourly total, mm
        self.relativais_mitrums_faktiskais = '4080'  # Relative humidity, actual, %
        self.relativais_mitrums_stundas_max = '4084'  # Relative humidity, hourly maximum, %
        self.relativais_mitrums_stundas_min = '4082'  # Relative humidity, hourly minimum, %
        self.saules_spidesanas_ilgums_stundas_summa = '4606'  # Sunshine duration, hourly total, hours
        self.sniega_segas_biezums_stundas_videjais = '4341'  # Snow cover thickness, hourly average, cm
        self.summara_radiacija_stundas_max = '4530'  # Total radiation, hourly maximum, W/m²
        self.summara_radiacija_stundas_min = '4528'  # Total radiation, hourly minimum, W/m²
        self.summara_radiacija_stundas_videja = '4527'  # Total radiation, hourly average, W/m²
        self.temp_zales_augstuma_min_starp_noverojumiem = '4494'  # Grass temperature, minimum between observation periods, °C
        self.temp_zales_augstuma_stundas_max = '9883'  # Grass temperature, hourly maximum, °C
        self.temp_zales_augstuma_stundas_min = '9881'  # Grass temperature, hourly minimum, °C
        self.temp_zales_augstuma_stundas_videja = '9884'  # Grass temperature, hourly average, °C
        self.veja_atrums_faktiskais = '4211'  # Wind speed, actual, m/s
        self.veja_atrums_stundas_min = '4216'  # Wind speed, hourly minimum, m/s
        self.veja_brazmas_max_starp_terminiem = '4212'  # Wind gusts, maximum between observation periods, m/s
        self.veja_brazmas_stundas_max = '4218'  # Wind gusts, hourly maximum, m/s
        self.veja_virziens_faktiskais = '10298'  # Wind direction, actual (10-minute average), °

# Example usage: Look up the station ID by name
# station_id = Stations().Jelgava
# print(f"The station ID for Jelgava is {station_id}")