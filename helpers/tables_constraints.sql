-- ERA5
ALTER TABLE public.era5 ADD CONSTRAINT check_min_max CHECK (min <= max);
ALTER TABLE public.era5 ADD CONSTRAINT check_mean CHECK (mean between min AND max);
ALTER TABLE public.era5 ADD CONSTRAINT check_median CHECK (median between min AND max);
ALTER TABLE public.era5 ADD CONSTRAINT check_std CHECK (std >= 0);
ALTER TABLE public.era5 ADD CONSTRAINT check_count CHECK (count >= 0);
ALTER TABLE public.era5 ADD CONSTRAINT check_adm_level CHECK (adm_level between 0 AND 4);
ALTER TABLE public.era5 ADD CONSTRAINT check_iso3 CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.era5 ADD CONSTRAINT check_valid_date CHECK (valid_date <= CURRENT_DATE);

-- FloodScan
ALTER TABLE public.floodscan ADD CONSTRAINT check_min_max CHECK (min <= max);
ALTER TABLE public.floodscan ADD CONSTRAINT check_mean CHECK (mean between min AND max);
ALTER TABLE public.floodscan ADD CONSTRAINT check_median CHECK (median between min AND max);
ALTER TABLE public.floodscan ADD CONSTRAINT check_std CHECK (std >= 0);
ALTER TABLE public.floodscan ADD CONSTRAINT check_count CHECK (count >= 0);
ALTER TABLE public.floodscan ADD CONSTRAINT check_adm_level CHECK (adm_level between 0 AND 4);
ALTER TABLE public.floodscan ADD CONSTRAINT check_iso3 CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.floodscan ADD CONSTRAINT check_valid_date CHECK (valid_date <= CURRENT_DATE);

-- IMERG
ALTER TABLE public.imerg ADD CONSTRAINT check_min_max CHECK (min <= max);
ALTER TABLE public.imerg ADD CONSTRAINT check_mean CHECK (mean between min AND max);
ALTER TABLE public.imerg ADD CONSTRAINT check_median CHECK (median between min AND max);
ALTER TABLE public.imerg ADD CONSTRAINT check_std CHECK (std >= 0);
ALTER TABLE public.imerg ADD CONSTRAINT check_count CHECK (count >= 0);
ALTER TABLE public.imerg ADD CONSTRAINT check_adm_level CHECK (adm_level between 0 AND 4);
ALTER TABLE public.imerg ADD CONSTRAINT check_iso3 CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.imerg ADD CONSTRAINT check_valid_date CHECK (valid_date <= CURRENT_DATE);


-- SEAS5
ALTER TABLE public.seas5 ADD CONSTRAINT check_min_max CHECK (min <= max);
ALTER TABLE public.seas5 ADD CONSTRAINT check_mean CHECK (mean between min AND max);
ALTER TABLE public.seas5 ADD CONSTRAINT check_median CHECK (median between min AND max);
ALTER TABLE public.seas5 ADD CONSTRAINT check_std CHECK (std >= 0);
ALTER TABLE public.seas5 ADD CONSTRAINT check_count CHECK (count >= 0);
ALTER TABLE public.seas5 ADD CONSTRAINT check_adm_level CHECK (adm_level between 0 AND 4);
ALTER TABLE public.seas5 ADD CONSTRAINT check_iso3 CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.seas5 ADD CONSTRAINT check_leadtime CHECK (leadtime between 0 AND 6);
ALTER TABLE public.seas5 ADD CONSTRAINT check_valid_date CHECK (valid_date >= issued_date);
ALTER TABLE public.seas5 ADD CONSTRAINT check_month_difference CHECK (leadtime = EXTRACT(YEAR FROM AGE(valid_date, issued_date)) * 12 +
    EXTRACT(MONTH FROM AGE(valid_date, issued_date)));
