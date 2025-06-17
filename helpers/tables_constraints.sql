-- ERA5
ALTER TABLE public.era5 ADD CHECK (min <= max);
ALTER TABLE public.era5 ADD CHECK (mean between min AND max);
ALTER TABLE public.era5 ADD CHECK (median between min AND max);
ALTER TABLE public.era5 ADD CHECK (std >= 0);
ALTER TABLE public.era5 ADD CHECK (count >= 0);
ALTER TABLE public.era5 ADD CHECK (adm_level between 0 AND 4);
ALTER TABLE public.era5 ADD CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.era5 ADD CHECK (valid_date <= CURRENT_DATE);

-- FloodScan
ALTER TABLE public.floodscan ADD CHECK (min <= max);
ALTER TABLE public.floodscan ADD CHECK (mean between min AND max);
ALTER TABLE public.floodscan ADD CHECK (median between min AND max);
ALTER TABLE public.floodscan ADD CHECK (std >= 0);
ALTER TABLE public.floodscan ADD CHECK (count >= 0);
ALTER TABLE public.floodscan ADD CHECK (adm_level between 0 AND 4);
ALTER TABLE public.floodscan ADD CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.floodscan ADD CHECK (valid_date <= CURRENT_DATE);

-- IMERG
ALTER TABLE public.imerg ADD CHECK (min <= max);
ALTER TABLE public.imerg ADD CHECK (mean between min AND max);
ALTER TABLE public.imerg ADD CHECK (median between min AND max);
ALTER TABLE public.imerg ADD CHECK (std >= 0);
ALTER TABLE public.imerg ADD CHECK (count >= 0);
ALTER TABLE public.imerg ADD CHECK (adm_level between 0 AND 4);
ALTER TABLE public.imerg ADD CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.imerg ADD CHECK (valid_date <= CURRENT_DATE);


-- SEAS5
ALTER TABLE public.seas5 ADD CHECK (min <= max);
ALTER TABLE public.seas5 ADD CHECK (mean between min AND max);
ALTER TABLE public.seas5 ADD CHECK (median between min AND max);
ALTER TABLE public.seas5 ADD CHECK (std >= 0);
ALTER TABLE public.seas5 ADD CHECK (count >= 0);
ALTER TABLE public.seas5 ADD CHECK (adm_level between 0 AND 4);
ALTER TABLE public.seas5 ADD CHECK (iso3 ~ '^[A-Z]{3}$');
ALTER TABLE public.seas5 ADD CHECK (median between min AND max);
ALTER TABLE public.seas5 ADD CHECK (leadtime between 0 AND 6);
ALTER TABLE public.seas5 ADD CHECK (valid_date >= issued_date);
ALTER TABLE public.seas5 ADD CHECK (leadtime = EXTRACT(YEAR FROM AGE(valid_date, issued_date)) * 12 +
    EXTRACT(MONTH FROM AGE(valid_date, issued_date)));
