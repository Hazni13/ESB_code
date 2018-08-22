create or replace PACKAGE BODY       R_DATOTEKA AS

  gc_module_name CONSTANT VARCHAR2(100) := 'R_DATOTEKA';
  g_profiler_run_id NUMBER;
  --UAT i PROD
  g_mail_to varchar2(200):= 'SluRegBlo@esb.hr;RegPro@esb.hr;sluitprod@esb.hr;maticni@esb.hr';
  --TEST
  --g_mail_to varchar2(200):= 'nkovacevic12@esb.hr;';
  g_mail_err varchar2(200):='maticni@esb.hr';


  g_csv_rep utl_file.file_type;
  g_csv_err utl_file.file_type;
  g_r_dat   utl_file.file_type;

  g_path_csv_rep VARCHAR2(200)  :=NULL;
  g_path_csv_err VARCHAR2(200)  :=NULL;
  g_path_r_dat   VARCHAR2(200)  :=NULL;

  g_name_r_dat   VARCHAR2(200)  :=NULL;
  g_name_r_dat_tmp   VARCHAR2(200)  :=NULL;
  g_name_csv_rep VARCHAR2(200)  :=NULL;
  g_name_csv_err VARCHAR2(200)  :=NULL;

  g_oracle_dir_rep  varchar2(100) := 'PL_OUT';
  g_oracle_dir_err  varchar2(100) := 'PL_OUT';
  g_oracle_dir_dat  varchar2(100) := 'FINA_OUT';

  g_okolje        VARCHAR2(50):=NULL;
  g_dodatak       VARCHAR2(500):=NULL;



  procedure p_salji_r (o_status  out number , i_datum number default null) AS
     c_action_name CONSTANT VARCHAR2(100) := 'P_SALJI_R';
     w_zapreg number:=0;
     w_systime varchar2(25);
     w_sysdate number:=0;
     w_datum_ispis varchar2(10):='';
     w_vrijeme_ispis varchar2(15):='';

     w_gen_depins number:=0;
     w_gen_bicadr char(11):=' ';

     w_datum number(8):=(case when nvl(i_datum,0)=0 then
                              to_number(to_char(sysdate,'YYYYMMDD'))
                           else i_datum
                      end);

     tt_account_details t_account_details := t_account_details();
     type t_err_accounts is table of partije.broj_partije%TYPE;
     tt_err_accounts    t_err_accounts := t_err_accounts();

     w_cnt_rec          number:=0;
     w_cnt_err          number:=0;
     w_cnt_all          number:=0;
     w_cnt_new          number:=0;
     w_cnt_chg          number:=0;


     w_novi_racun       number:=0;
     w_cnt_potpisnika   number:=0;

     w_control_err      number:=0;
     w_control_override number:=0;
     w_descrip_err      varchar2(100):='';

     riv_slog varchar2(400):='';
     ris_slog varchar2(400):='';
     riz_slog varchar2(400):='';

     err_slog varchar2(100):='';
     rep_slog varchar2(1000):='';


     cursor c_accnt_record is
        (select distinct
          par.broj_partije      broj_racuna,
          par.iban_broj      iban,
          kli.maticni_broj    maticni_broj,
          kli.OIB_NUM_DIO     oib,
          translate(trim(ezr.skraceni_naziv),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158))  nziv,
          --trim(ezr.skraceni_naziv)nziv,
          mje.zupanija        zupanija,
          mje.opcina          opcina,
          translate(trim(adr.adresa||' '||adr.kucni_broj||nvl2(adr.dodatni_kucni_broj,'/'||adr.dodatni_kucni_broj,null)),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)) adresa,
          translate(trim(mje.MJESTO),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158))  mjesto,
          --trim(mje.MJESTO)    mjesto,
          to_char(par.vazi_od,'YYYY-MM-DD')vazi_od,
          case
             when par.vazi_do is not null then
               to_char(par.vazi_do,'YYYY-MM-DD')
             else
               ' '
          end vazi_do,
          1 pripadnost,
          (select sifra from poslovnice_fine where id = par.pjf_id) mjesto_izvoda,
          SUBSTR(LPAD(fina_servisi_loader.fina_tip_izvoda(par.broj_partije), 2, ' '), 2, 1) tip_izvoda,
          DECODE(nvl(ezr.vidljivo_stanje,0), 0, 1, 2) vipkli,
          (select sifra from poslovnice_fine where id = par.pjf_id) pj_fine,
          --par.par_slijed      slijednik,
          substr(paribn.iban_broj,1,21) slijednik,
          ezr.datum_promjene  dat_prom,
          ezr.azurirao        inicijali,
          porg.par_sektor     sektor,
          porg.par_centar     centar,
          porg.par_sluzba     sluzba,
          porg.par_lokacija   orgjed,
          porg.par_tim        siftim,
          (select sifra from lokacije where id = par.lkc_id) posjed,
          (select maticni_broj from klijenti where id = par.kli_id_upravitelja) matupr,
          ezr.tip_namjene     namjen,
          par.OZNAKA_BLOKADE  sporni,
          par.vazi_od     vazi_od_num,
          par.vazi_do    vazi_do_num,
          par.id              id
       from partije par,
         partije paribn,
         partije_v_org_prip porg,
         klijenti kli,
         EVIDENCIJE_ZIRORACUNA ezr,
         vrste_posla vp,
         table(KLIJENTI_OSNOVNI_PODACI.f_get_adrese_podaci_tt(i_nEzrId=>ezr.id, i_nTipAdresa=>91, i_nAktivne=>1)) adr,
         ( select mje.naziv mjesto, opg.maticni_broj opcina, zup.sifra zupanija, mje.id
           from ibis.mjesta mje,
                opcine_gradovi opg,
                zupanije zup
           WHERE MJE.OPG_ID = OPG.ID AND OPG.ZUP_ID = ZUP.ID
         ) mje
    where  par.kli_id = KLI.ID
       AND par.ZRI_ID = EZR.ID
       AND porg.par_id(+) = par.id
       and par.par_id_slijednik = paribn.id(+)
       AND par.vpa_id = VP.ID
       AND adr.mjesto_id = MJE.id(+)
       and kli.tip_klijenta in (1,3)
       AND VRSTE_POSLA_COMMON.F_DOHVAT_TIPA_VP(vp.id) IN (3,4)
       and konta_podaci.f_get_detaljni_id_d(vp.kto_id, 'STATUS_VALUTE') = 0
       AND SUBSTR(PAR.broj_partije,1,2) IN (11,13,14,15,18)
       and (par.vazi_do is null or
           (par.vazi_do is not null  and par.vazi_do >= trunc(nvl(par.datum_slanja_r_datoteke, to_date('00010101','yyyymmdd')))))
       --and trim(par.vazi_od) <> trim(par.vazi_do)
       --VR1 dorada 21.03.2016
       and trunc(nvl(par.vazi_od, to_date('00010101','yyyymmdd'))) <> trunc(nvl(par.vazi_do, to_date('00010101','yyyymmdd')))
       and nvl(par.INDIKATOR_SLANJA_R_DATOTEKE,0) in (0,5,6)
       and par.INDIKATOR_AKTIVNOSTI = 0
       and par.OZNAKA_AKTIVNOSTI_JRR = 0)
   UNION
       (
          select distinct
          par.broj_partije      broj_racuna,
          par.iban_broj      iban,
          0    maticni_broj,
          kli.OIB_NUM_DIO     oib,
          substr(translate(trim(adr.opis),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)),1,70)  nziv,
          --substr(trim(exi.exi_opisno),1,70)  nziv,
          exi.zupanija        zupanija,
          exi.opcina          opcina,
          translate(trim(adr.adresa||' '||adr.kucni_broj||nvl2(adr.dodatni_kucni_broj,'/'||adr.dodatni_kucni_broj,null)),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)) adresa,
          translate(trim(exi.mjesto),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)) mjesto,
          to_char(par.vazi_od,'YYYY-MM-DD') vazi_od,
          case
             when par.vazi_do is not null then
               to_char(par.vazi_do, 'YYYY-MM-DD')
             else
               ' '
          end vazi_do,
          1 pripadnost,
          (select sifra from poslovnice_fine where id = par.pjf_id) mjesto_izvoda,
          SUBSTR(LPAD(fina_servisi_loader.fina_tip_izvoda(par.broj_partije), 2, ' '), 2, 1) tip_izvoda,
          1 as vipkli, --VR1 def 482619
          (select sifra from poslovnice_fine where id = par.pjf_id)   pj_fine,
           --par.par_slijed      slijednik,
          substr(paribn.iban_broj,1,21) slijednik,
          par.datum_azuriranja      dat_prom,
          par.azurirao      inicijali,
          porg.par_sektor      sektor,
          porg.par_centar      centar,
          porg.par_sluzba      sluzba,
          porg.par_lokacija    orgjed,
          porg.par_tim         siftim,
          (select sifra from lokacije where id = par.lkc_id)   posjed,
          (select maticni_broj from klijenti where id = par.kli_id_upravitelja) matupr,
          null as namjen, --ezr.tip_namjene     namjen,
          par.oznaka_blokade  sporni,
          par.vazi_od     vazi_od_num,
          par.vazi_do     vazi_do_num,
          par.id
       from partije par,
         partije paribn,
         klijenti kli,
         --EVIDENCIJE_ZIRORACUNA ezr,
         vrste_posla vp,
         table(KLIJENTI_OSNOVNI_PODACI.f_get_adrese_podaci_tt(i_nparId=>par.id, i_nTipAdresa=>91, i_nAktivne=>1)) adr,
         ( select mje.naziv mjesto, opg.maticni_broj opcina, zup.sifra zupanija, mje.id
           from ibis.mjesta mje,
                opcine_gradovi opg,
                zupanije zup
           WHERE MJE.OPG_ID = OPG.ID AND OPG.ZUP_ID = ZUP.ID
         ) exi,
         partije_v_org_prip porg
/*
         (select distinct exi.*
                 --,ROW_NUMBER() OVER (PARTITION BY exi.exi_mjesto ORDER BY exi.exi_extinf) rn
                 FROM ( (select exi_extinf,exi_opisno, exi_adresa, exi_mjesto, opg.maticni_broj opcina, zup.SIFRA ZUPANIJA from ibis.mjesta mje,opcine_gradovi opg,zupanije zup, EXTINF exi
          WHERE mje.opg_id = opg.id and opg.zup_id = zup.id and trim(upper(mje.naziv)) = trim(upper(exi.exi_mjesto))
          and mje.vazi_do is null ) ORDER BY EXI_EXTINF) exi) exi
          */
       where  par.kli_id = KLI.ID
         --and par.par_extinf = exi.exi_extinf
         and exi.id = adr.mjesto_id
         and porg.par_id(+) = par.id
        -- and exi.rn = 1
        and vp.id = par.vpa_id
       --------AND par.zri_id = EZR.ID (+)
       and par.par_id_slijednik = paribn.id(+)

       and kli.tip_klijenta in (0)
       and VRSTE_POSLA_COMMON.F_DOHVAT_TIPA_VP(vp.id) IN (3,4)
       AND konta_podaci.f_get_detaljni_id_d(vp.kto_id, 'STATUS_VALUTE') = 0
       --AND OPD.OPD_VODBRO IN (11,13,14,15,18)
       AND SUBSTR(par.broj_partije,1,2) IN (35)
       and (par.vazi_do is null or
           (par.vazi_do is not null and par.vazi_do >= trunc(nvl(par.datum_slanja_r_datoteke, to_date('00010101','yyyymmdd')))))
       and trunc(nvl(par.vazi_od, to_date('00010101','yyyymmdd'))) <> trunc(nvl(par.vazi_do, to_date('00010101','yyyymmdd')))
       and par.indikator_slanja_r_datoteke in (0,5,6)
       and par.indikator_aktivnosti = 0
       and nvl(par.oznaka_aktivnosti_jrr,0) = 0);

  BEGIN
     o_status :=0;
     --dbms_application_info.set_module(module_name => gc_module_name, action_name => c_action_name);
     --g_profiler_run_id  := dbms_profiler.start_profiler(run_comment => gc_module_name, run_comment1 => c_action_name);

     begin

        W_GEN_DEPINS:=PARAMETRI_BANKE_PODACI.f_banka_podaci('SIFRA_DEPOZITNE_INST');
        w_gen_bicadr:=PARAMETRI_BANKE_PODACI.f_banka_podaci('ADRESA_SWIFT');
     EXCEPTION
        WHEN OTHERS THEN
           w_gen_depins:=2402006;
           w_gen_bicadr:='ESBCHR22   ';
     END;

     SELECT to_char(SYSDATE,'DD.MM.YYYY HH24:mi:ss'), TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')), to_char(sysdate,'YYYY-MM-DD'), to_char(systimestamp,'hh24.mi.ss.FF')
       INTO w_systime, w_sysdate, w_datum_ispis, w_vrijeme_ispis
     FROM DUAL;

     BEGIN
        select upper(trim(cs.value)) into g_okolje from common.settings cs where cs.setting_name = 'ENVIRONMENT_ID';
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           g_okolje:='UAT';
     END;

     common.utils.send_email('(EBC_IREF_'||g_okolje||') BOT '||gc_module_name||'.'||c_action_name ,
                             g_mail_err ,
                             'Program krenuo :'|| w_systime||' sa paramterima: '||w_datum,
                             'FaxServer@esb.hr;', '' ,'text/plain', '', 'text/plain', '');


     BEGIN
        SELECT BROJCANA_VRIJEDNOST+1 INTO W_ZAPREG FROM FIKSNI WHERE OPISNA_SIFRA = 'ZAPREG';
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           w_zapreg:=0;
     END;

     -------------------------------------------------------------------------------------------------
     -- Definiranje naziva datoteke, inicijalizacija DEPHDR-a i sl.
     -------------------------------------------------------------------------------------------------
     g_dodatak   := userenv('sessionid');
     -------------------------------------------------------------------------------------------------
     -- Kreiranje naziva datoteke za FINU - ista ne smije imati DEPHDR i u njoj se mora rucno raditi
     -- konverzija znakova na windows 1250 kodnu stranicu
     -------------------------------------------------------------------------------------------------
     g_name_r_dat := 'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'.BNK';
     g_name_r_dat_tmp := 'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'.TMP';
     g_r_dat    := utl_file.fopen(g_oracle_dir_dat,g_name_r_dat_tmp,'W');

     g_name_csv_rep := 'POSLANO_'||'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'_'||g_dodatak||'.csv';
     g_csv_rep  := utl_file.fopen(g_oracle_dir_rep,g_name_csv_rep,'W');


     g_name_csv_err := 'GRESKE_'||'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'_'||g_dodatak||'.csv';
     g_csv_err  := utl_file.fopen(g_oracle_dir_err,g_name_csv_err,'W');

     -------------------------------------------------------------------------------------------------
     -- Definiranje log datoteke, inicijalizacija DEPHDR-a i sl.
     -------------------------------------------------------------------------------------------------

     g_path_csv_rep  := 'R_DATOTEKA/' || w_datum || '/' || TO_CHAR(dbms_random.value(1, 100000000), 'FM00000000', 'NLS_NUMERIC_CHARACTERS = '',.''') || '/';
     -------------------------------------------------------------------------------------------------
     utl_file.put_line(g_csv_rep, '$DEPHDR$/' || g_path_csv_rep || g_name_csv_rep);
     utl_file.put_line(g_csv_rep,'SLANJE U JRIR ZA DATUM:'||w_datum_ispis);
     utl_file.put_line(g_csv_rep,';');
     utl_file.put_line(g_csv_rep, 'JMBG;PARTIJA;POS;MATBRO;OIB;NAZIV;NAMJ.STARA;NAMJ.NOVA;BLOK.STARA;BLOK.NOVA;TIP PRIJENOSA;VAZI_DO;INICIJALI KORISNIKA;SEK;CEN;SLU;ORG;TIM');

     -------------------------------------------------------------------------------------------------
     -- Definiranje datoteke rezultata slanja koja sluzi kao kontrola
     -------------------------------------------------------------------------------------------------
     g_path_csv_err  := 'R_DATOTEKA/' || w_datum || '/' || TO_CHAR(dbms_random.value(1, 100000000), 'FM00000000', 'NLS_NUMERIC_CHARACTERS = '',.''') || '/';
     -------------------------------------------------------------------------------------------------
     utl_file.put_line(g_csv_err, '$DEPHDR$/' || g_path_csv_err || g_name_csv_err);
     utl_file.put_line(g_csv_err,'Partija;MB klijenta;Ima grešku;Opis greške;Dat.Pro;Inicijali');
    -------------------------------------------------------------------------------------------------
     riv_slog:=rpad('RIV'||w_gen_depins||w_datum_ispis||'-'||w_vrijeme_ispis||'7777779'||'R2402'||w_datum||lpad(W_ZAPREG,5,0),399,' ')||'V';
     utl_file.put_line(g_r_dat,riv_slog);


     if c_accnt_record%ISOPEN then
        close c_accnt_record;
     end if;

     open c_accnt_record;
     loop
        fetch c_accnt_record bulk collect into tt_account_details limit 500;
        begin
           forall i in indices of tt_account_details save exceptions
              update partije set indikator_slanja_r_datoteke = 5 where broj_partije = tt_account_details(i).p_broj_racuna;
              commit;
        exception
           when OTHERS then
              for i in 1..sql%bulk_exceptions.count
              loop
                 common.zapisi_log('Exception Updating Par_sinkro Partija '
                        , sql%bulk_exceptions(i).error_code
                        , dbms_utility.format_error_stack
                        , sqlerrm(-sql%bulk_exceptions(i).error_code)||dbms_utility.format_error_backtrace
                        , 'R_DATOTEKA.P_SALJI_R' );
              end loop;
        end;

        for i in 1..tt_account_details.count
        loop
           begin
              w_cnt_all := w_cnt_all +1;
              w_control_err:=0;
              w_control_override:=0;
              w_descrip_err:='';
              w_novi_racun:=0;

              select count(*) into w_novi_racun from registar_racuna where broj_racuna like '%2402006'||tt_account_details(i).p_broj_racuna;
              --VR1 ukinuto zbog duplog brojanja grešaka
              --if w_novi_racun > 0 then
              --   w_cnt_chg := w_cnt_chg + 1;
              --else
              --   w_cnt_new := w_cnt_new + 1;
              --end if;

              --VR1 defect 440916
              if substr(to_char(tt_account_details(i).p_broj_racuna),1,2) = '35' then
                 select count(*) into w_cnt_potpisnika from POTPISNICI WHERE PAR_ID = tt_account_details(i).p_par_id AND TIPOVI_OVLASTI = 0 AND vazi_do is null;--VR1 nije TIP_POTPISA nego TIPOVI_OVLASTI = 0 --potpisnik
              else
                 select count(*) into w_cnt_potpisnika from potpisnici where zri_id in (select id from evidencije_ziroracuna where vbdi = PARAMETRI_BANKE_PODACI.f_banka_podaci('SIFRA_DEPOZITNE_INST') and broj_partije = tt_account_details(i).p_broj_racuna) and TIPOVI_OVLASTI = 0 and vazi_do is null;--VR1 nije TIP_POTPISA nego TIPOVI_OVLASTI = 0 --potpisnik
              end if;

              if w_cnt_potpisnika = 0 then
                 w_control_override :=1;
                 w_control_err:=1;
                 w_descrip_err:='NISU UNESENI POTPISNICI';
              end if;

              if trim(tt_account_details(i).p_naziv) is null then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNAT NAZIV';
              end if;

              if trim(tt_account_details(i).p_mjesto) is null then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATO MJESTO';
              end if;

              if trim(tt_account_details(i).p_adresa) is null then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATA ADRESA';
              end if;


              if tt_account_details(i).p_opcina = 0 then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATA OPĆINA';
              end if;

              if tt_account_details(i).p_zupanija = 0 then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATA ŽUPANIJA';
              end if;


              if w_control_err > 0 and w_control_override = 0 then
                 w_cnt_err := w_cnt_err +1;
                 --w_cnt_chg := w_cnt_chg + 1;

                 err_slog := tt_account_details(i).p_broj_racuna ||';'||
                             tt_account_details(i).p_maticni_broj||';'||
                                                              ' '||';'||
                                                    w_descrip_err||';'||
                               tt_account_details(i).p_datum_prom||';'||
                               tt_account_details(i).p_inicijali;
                 utl_file.put_line(g_csv_err,err_slog);
                 err_slog:='';
              else
                 if w_control_override = 1 then
                    err_slog := tt_account_details(i).p_broj_racuna ||';'||
                                tt_account_details(i).p_maticni_broj||';'||
                                                              ' '||';'||
                                                     w_descrip_err||';'||
                                tt_account_details(i).p_datum_prom||';'||
                                tt_account_details(i).p_inicijali;
                    utl_file.put_line(g_csv_err,err_slog);
                    err_slog:='';
                 end if;
                 ris_slog:=rpad('RIS'||w_gen_bicadr||w_gen_depins
                                     ||tt_account_details(i).p_iban||lpad(tt_account_details(i).p_maticni_broj,8,0)
                                     ||lpad(tt_account_details(i).p_oib,11,0)||rpad(tt_account_details(i).p_naziv,70,' ')
                                     ||lpad(tt_account_details(i).p_zupanija,2,0)||lpad(tt_account_details(i).p_opcina,4,0)
                                     ||rpad(tt_account_details(i).p_adresa,70,' ')||rpad(tt_account_details(i).p_mjesto,70,' ')
                                     ||rpad(tt_account_details(i).p_vazi_od,10,' ')||rpad(tt_account_details(i).p_vazi_do,10,' ')
                                     ||tt_account_details(i).p_rac_prip
                                     ||lpad(case when nvl(tt_account_details(i).p_izv_tip,'0') = '8' then ' ' else  lpad(case when nvl(tt_account_details(i).p_izv_mje,0) = 0 then ' ' else to_char(tt_account_details(i).p_izv_mje) end,5,' ') end,5,' ')
                                     ||tt_account_details(i).p_izv_tip||tt_account_details(i).p_vipkli
                                     ||lpad(case when nvl(tt_account_details(i).p_pjfine,0) = 0 then ' ' else to_char(tt_account_details(i).p_pjfine) end ,5,' ')
                                     ||rpad(case when trim(tt_account_details(i).p_slijednik) = '0' then ' ' else trim(tt_account_details(i).p_slijednik) end ,21,' ')
                                    ,399,' ')||'S';
                 utl_file.put_line(g_r_dat,ris_slog);
                 w_cnt_rec := w_cnt_rec+1;
                 if w_novi_racun > 0 then
                    w_cnt_chg := w_cnt_chg + 1;
                 else
                    w_cnt_new := w_cnt_new + 1;
                 end if;
                 ris_slog:=' ';
                 rep_slog:=(tt_account_details(i).p_matupr||';'||tt_account_details(i).p_broj_racuna||';'||
                            tt_account_details(i).p_posjed||';'||tt_account_details(i).p_maticni_broj||';'||
                            tt_account_details(i).p_oib||';'||tt_account_details(i).p_naziv||';'||
                            ' '||';'||
                            case when tt_account_details(i).p_namjen = 1 then 'NAMIRENJE'
                                 else 'OBIČAN'
                            end ||';'||
                            case when tt_account_details(i).p_sporni = 2 then 'BLOKIRAN'
                                 else 'AKTIVAN'
                            end ||';'||
                            case when tt_account_details(i).p_sporni = 2 then 'BLOKIRAN'
                                 else 'AKTIVAN'
                            end ||';'||
                            case when tt_account_details(i).p_par_vazido is not null then
                                    'ZATVARANJE'
                                 else
                                    case when w_novi_racun = 0 then
                                         'OTVARANJE'
                                       else
                                         'PROMJENA'
                                    end
                            end ||';'||
                            tt_account_details(i).p_vazi_do||';'||tt_account_details(i).p_inicijali||';'||
                            tt_account_details(i).p_sektor||';'||tt_account_details(i).p_centar||';'||
                            tt_account_details(i).p_sluzba||';'||tt_account_details(i).p_orgjed||';'||
                            tt_account_details(i).p_siftim||';'
                            );
                 utl_file.put_line(g_csv_rep,rep_slog);
                 rep_slog:='';
              end if;
           end;
        end loop;

        begin
           forall i in indices of tt_account_details save exceptions
              update partije set indikator_slanja_r_datoteke = 8
              where broj_partije = tt_account_details(i).p_broj_racuna;
        exception
           when OTHERS then
              for i in 1..sql%bulk_exceptions.count
              loop
                 common.zapisi_log('Exception while Updating Partija Table-' || tt_account_details(i).p_broj_racuna
                                  , sql%bulk_exceptions(i).error_code
                                  , dbms_utility.format_error_stack
                                  , sqlerrm(-sql%bulk_exceptions(i).error_code)||dbms_utility.format_error_backtrace
                                 , 'R_DATOTEKA.P_SALJI_R' );
              end loop;
        end;

        --rollback;
        commit;

        exit when tt_account_details.count < 500;
     END LOOP;

     close c_accnt_record;

     BEGIN
        UPDATE FIKSNI SET BROJCANA_VRIJEDNOST = W_ZAPREG  WHERE OPISNA_SIFRA = 'ZAPREG';
        --ROLLBACK;
        COMMIT;
     EXCEPTION
        WHEN OTHERS THEN
           w_zapreg:=0;
     END;

     SELECT to_char(SYSDATE,'DD.MM.YYYY HH24:mi:ss'), TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')),  to_char(systimestamp,'hh24.mi.ss.FF')
       INTO w_systime, w_sysdate, w_vrijeme_ispis
     FROM DUAL;

     riz_slog:=rpad('RIZ'||w_gen_depins||w_datum_ispis||'-'||w_vrijeme_ispis||lpad(w_cnt_rec+2,6,0),399,' ')||'Z';
     utl_file.put_line(g_r_dat,riz_slog);
     riz_slog:=rpad(' ',399,' ')||'P';
     utl_file.put_line(g_r_dat,riz_slog);

     utl_file.put_line(g_csv_err,' ');
     utl_file.put_line(g_csv_err,' ');
     err_slog:='POSLANIH: '||w_cnt_rec;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='PROMJENA: '||w_cnt_chg;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='NOVIH: '||w_cnt_new;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='GRESAKA: '||w_cnt_err;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='UKUPNO: '||w_cnt_all;
     utl_file.put_line(g_csv_err,err_slog);

     if utl_file.is_open(g_r_dat) then
        utl_file.fclose(g_r_dat);
        utl_file.frename(g_oracle_dir_dat,g_name_r_dat_tmp,g_oracle_dir_dat,g_name_r_dat,TRUE);
     end if;

     if utl_file.is_open(g_csv_err) then
        utl_file.fclose(g_csv_err);
     end if;

     if utl_file.is_open(g_csv_rep) then
        utl_file.fclose(g_csv_rep);
     end if;

     --------------------------------------------------------------------------------------------------------------------
     --SLANJE MAILOVA OBAVJESTI S LISTAMA PRIJENOSA I LISTAMA GRESAKA I MAIL O KRAJU PROGRAMA
     --------------------------------------------------------------------------------------------------------------------
     common.utils.depcon_dephdr('PL_OUT',g_name_csv_rep);
     COMMON.UTILS.SEND_EMAIL('(EBC_'||g_okolje||') R DATOTEKA - lista prijenosa' ,  --subject
                              g_mail_to , --mailto
                              chr(13)||chr(10)||'Postovani/a izvjestaj se nalazi na linku: '||common.utils.getsettingsbyname('DEPHDR_PATH')|| REPLACE(g_path_csv_rep, '/', '\') ||chr(13)||chr(10)||
                              chr(13)||chr(10)||'Molimo Vas da tabelu snimite lokalno kod sebe jer ce ista biti obrisana u roku od tjedan dana. Hvala!' ,  --mailbody
                              'FaxServer@esb.hr',  --mailfrom
                              '' ,
                              'text/plain',
                              '',
                              'text/plain',
                              '' ) ;

     common.utils.depcon_dephdr('PL_OUT', g_name_csv_err);
     COMMON.UTILS.SEND_EMAIL('(EBC_'||g_okolje||') R DATOTEKA - greske' ,  --subject
                              g_mail_to , --mailto
                              chr(13)||chr(10)||'Postovani/a izvjestaj se nalazi na linku: '||common.utils.getsettingsbyname('DEPHDR_PATH')|| REPLACE(g_path_csv_err, '/', '\') ||chr(13)||chr(10)||
                              chr(13)||chr(10)||'Molimo Vas da tabelu snimite lokalno kod sebe jer ce ista biti obrisana u roku od tjedan dana. Hvala!' ,  --mailbody
                              'FaxServer@esb.hr',  --mailfrom
                              '' ,
                              'text/plain',
                              '',
                              'text/plain',
                              '' ) ;

     common.utils.send_email('(EBC_'||g_okolje||') EOT '||gc_module_name||'.'||c_action_name ,
                              g_mail_err ,
                              'Program zavrsio :'||w_systime ||' sa paramterima: '||w_datum,
                              'FaxServer@esb.hr;', '' ,'text/plain', '', 'text/plain', '');

  EXCEPTION
     WHEN OTHERS THEN
        o_status := 42; --greska u programu, javi TWSu da je otislo kjarcu
        common.zapisi_log(i_comment  => c_action_name,
                          i_sqlcode  => sqlcode,
                          i_sqlerror => DBMS_UTILITY.FORMAT_ERROR_STACK,
                          i_errstack => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                          i_imepro   => gc_module_name);
        common.utils.send_email('(EBC_'||g_okolje||') ERROR '||gc_module_name||'.'||c_action_name ,
                                  g_mail_err ,
                                  'Greska u  '||gc_module_name||'.'||c_action_name||'. Tip greske: '||sqlerrm||' : '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                                  'FaxServer@esb.hr;', '' ,'text/plain', '', 'text/plain', '');

        if utl_file.is_open(g_r_dat) then
           utl_file.fclose(g_r_dat);
        end if;

        if utl_file.is_open(g_csv_err) then
           utl_file.fclose(g_csv_err);
        end if;

        if utl_file.is_open(g_csv_rep) then
           utl_file.fclose(g_csv_rep);
        end if;

  END p_salji_r;

/*  procedure p_salji_r_init (o_status  out number , i_datum number default null) AS
     c_action_name CONSTANT VARCHAR2(100) := 'P_SALJI_R_INIT';
     w_zapreg number:=0;
     w_systime varchar2(25);
     w_sysdate number:=0;
     w_datum_ispis varchar2(10):='';
     w_vrijeme_ispis varchar2(15):='';

     w_gen_depins number:=0;
     w_gen_bicadr char(11):=' ';

     w_datum number(8):=(case when nvl(i_datum,0)=0 then
                              to_number(to_char(sysdate,'YYYYMMDD'))
                           else i_datum
                      end);

     tt_account_details t_account_details := t_account_details();
     type t_err_accounts is table of partija.par_sifpar%TYPE;
     tt_err_accounts    t_err_accounts := t_err_accounts();

     w_cnt_rec          number:=0; --count of all records for sending
     w_cnt_rec_file     number:=0; --count of records in one file
     w_cnt_err          number:=0;
     w_cnt_all          number:=0;
     w_cnt_new          number:=0;
     w_cnt_chg          number:=0;


     w_novi_racun       number:=0;
     w_cnt_potpisnika   number:=0;

     w_control_err      number:=0;
     w_control_override number:=0;
     w_descrip_err      varchar2(100):='';

     riv_slog varchar2(400):='';
     ris_slog varchar2(400):='';
     riz_slog varchar2(400):='';

     err_slog varchar2(100):='';
     rep_slog varchar2(1000):='';


     cursor c_accnt_record is
        (select distinct
          par.par_sifpar      broj_racuna,
          par.par_ibanbr      iban,
          kli.maticni_broj    maticni_broj,
          kli.OIB_NUM_DIO     oib,
          translate(trim(ezr.skraceni_naziv),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158))  nziv,
          --trim(ezr.skraceni_naziv)nziv,
          mje.zupanija        zupanija,
          mje.opcina          opcina,
          translate(trim(ezr.naziv_adrese||' '||ezr.kucni_broj||nvl2(ezr.dodatni_kucni_broj,'/'||ezr.dodatni_kucni_broj,null)),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)) adresa,
          translate(trim(mje.MJESTO),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158))  mjesto,
          --trim(mje.MJESTO)    mjesto,
          to_char(to_Date(par.par_vaziod,'YYYYMMDD'),'YYYY-MM-DD')vazi_od,
          case
             when par.par_vazido > 0 then
               to_char(to_Date(par.par_vazido,'YYYYMMDD'),'YYYY-MM-DD')
             else
               ' '
          end vazi_do,
          1 pripadnost,
          par.par_pjfine mjesto_izvoda,
          SUBSTR(LPAD(fina_servisi_loader.fina_tip_izvoda(par.par_sifpar), 2, ' '), 2, 1) tip_izvoda,
          DECODE(nvl(ezr.vidljivo_stanje,0), 0, 1, 2) vipkli,
          par.par_pjfine      pj_fine,
          --par.par_slijed      slijednik,
          substr(paribn.PAR_IBANBR,1,21) slijednik,
          ezr.datum_promjene  dat_prom,
          ezr.azurirao        inicijali,
          par.par_sektor      sektor,
          par.par_centar      centar,
          par.par_sluzba      sluzba,
          par.par_orgjed      orgjed,
          par.par_siftim      siftim,
          par.par_posjed      posjed,
          par.par_matupr      matupr,
          ezr.tip_namjene     namjen,
          par.par_sporni      sporni,
          par.par_vaziod      vazi_od_num,
          par.par_vazido      vazi_do_num
       from partija par,partija paribn,
         klijenti kli,
         EVIDENCIJE_ZIRORACUNA ezr,
         opcidep opd,
         ( select mje.naziv mjesto, opg.maticni_broj opcina, zup.sifra zupanija, mje.id
           from ibis.mjesta mje,
                opcine_gradovi opg,
                zupanije zup
           WHERE MJE.OPG_ID = OPG.ID AND OPG.ZUP_ID = ZUP.ID
         ) mje
    where  par.kli_id = KLI.ID
       and par.par_slijed = paribn.par_sifpar(+)
       AND par.EZR_ID = EZR.ID
       AND par.PAR_VPOSLA = OPD.OPD_SIFDEP
       AND ezr.MJE_ID = MJE.id(+)
       and par.par_kdsgra in (1,3)
       and OPD.OPD_TIPUPL in(3,4)
       AND OPD.OPD_STATUS = 0
       --AND OPD.OPD_VODBRO IN (11,13,14,15,18)
       AND SUBSTR(PAR.PAR_SIFPAR,1,2) IN (11,13,14,15,18)
       and par.par_vazido = 0
       and par.par_datreg > 0
       and par.par_sinkro > 0
       and par.par_indpro = 0
       and nvl(par.par_jrrsla,0) = 0)
       union
       (        select distinct
          par.par_sifpar      broj_racuna,
          par.par_ibanbr      iban,
          0    maticni_broj,
          kli.OIB_NUM_DIO     oib,
          substr(translate(trim(exi.exi_opisno),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)),1,70)  nziv,
          --substr(trim(exi.exi_opisno),1,70)  nziv,
          exi.zupanija        zupanija,
          exi.opcina          opcina,
          translate(trim(exi.exi_adresa),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158)) adresa,
          translate(trim(exi.exi_MJESTO),'ŠŽšž', CHR(138)||CHR(142)||CHR(154)||CHR(158))    mjesto,
          to_char(to_Date(par.par_vaziod,'YYYYMMDD'),'YYYY-MM-DD')vazi_od,
          case
             when par.par_vazido > 0 then
               to_char(to_Date(par.par_vazido,'YYYYMMDD'),'YYYY-MM-DD')
             else
               ' '
          end vazi_do,
          1 pripadnost,
          par.par_pjfine mjesto_izvoda,
          SUBSTR(LPAD(fina_servisi_loader.fina_tip_izvoda(par.par_sifpar), 2, ' '), 2, 1) tip_izvoda,
          DECODE(nvl(ezr.vidljivo_stanje,0), 0, 1, 2) vipkli,
          par.par_pjfine      pj_fine,
           --par.par_slijed      slijednik,
          substr(paribn.PAR_IBANBR,1,21) slijednik,
          case when par.par_update > 0 then
             to_date(par.par_update,'YYYYMMDD')
          else
             null
          end                 dat_prom,
          par.par_inicia      inicijali,
          par.par_sektor      sektor,
          par.par_centar      centar,
          par.par_sluzba      sluzba,
          par.par_orgjed      orgjed,
          par.par_siftim      siftim,
          par.par_posjed      posjed,
          par.par_matupr      matupr,
          ezr.tip_namjene     namjen,
          par.par_sporni      sporni,
          par.par_vaziod      vazi_od_num,
          par.par_vazido      vazi_do_num
       from partija par,partija paribn,
         klijenti kli,
         EVIDENCIJE_ZIRORACUNA ezr,
         opcidep opd,
         (select distinct exi.*
                 --,ROW_NUMBER() OVER (PARTITION BY exi.exi_mjesto ORDER BY exi.exi_extinf) rn
                 FROM ( (select exi_extinf,exi_opisno, exi_adresa, exi_mjesto, opg.maticni_broj opcina, zup.SIFRA ZUPANIJA from ibis.mjesta mje,opcine_gradovi opg,zupanije zup,EXTINF exi
          WHERE mje.opg_id = opg.id and opg.zup_id = zup.id and trim(upper(mje.naziv)) = trim(upper(exi.exi_mjesto))
          and mje.vazi_do is null ) ORDER BY EXI_EXTINF) exi) exi
       where  par.kli_id = KLI.ID
         and par.par_extinf = exi.exi_extinf
        -- and exi.rn = 1
       AND par.EZR_ID = EZR.ID (+)
       and par.par_slijed = paribn.par_sifpar(+)
       AND par.PAR_VPOSLA = OPD.OPD_SIFDEP
       and par.par_kdsgra in (0)
       and OPD.OPD_TIPUPL in(3,4)
       AND OPD.OPD_STATUS = 0
       --AND OPD.OPD_VODBRO IN (11,13,14,15,18)
       AND SUBSTR(PAR.PAR_SIFPAR,1,2) IN (35)
       and par.par_vazido = 0
       and par.par_datreg > 0
       and par.par_sinkro > 0
       and par.par_indpro = 0
       and nvl(par.par_jrrsla,0) = 0
       AND par.PAR_EXTINF > 0)
       ;


  BEGIN
     o_status :=0;
     --dbms_application_info.set_module(module_name => gc_module_name, action_name => c_action_name);
     --g_profiler_run_id  := dbms_profiler.start_profiler(run_comment => gc_module_name, run_comment1 => c_action_name);

     begin
        select GEN_DEPINS, gen_bicadr INTO W_GEN_DEPINS,w_gen_bicadr FROM GENERAL;
     EXCEPTION
        WHEN OTHERS THEN
           w_gen_depins:=2402006;
           w_gen_bicadr:='ESBCHR22   ';
     END;

     SELECT to_char(SYSDATE,'DD.MM.YYYY HH24:mi:ss'), TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')), to_char(sysdate,'YYYY-MM-DD'), to_char(systimestamp,'hh24.mi.ss.FF')
       INTO w_systime, w_sysdate, w_datum_ispis, w_vrijeme_ispis
     FROM DUAL;

     BEGIN
        select upper(trim(cs.value)) into g_okolje from common.settings cs where cs.setting_name = 'ENVIRONMENT_ID';
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           g_okolje:='UAT';
     END;

     common.utils.send_email('(EBC_'||g_okolje||') BOT '||gc_module_name||'.'||c_action_name ,
                             g_mail_err ,
                             'Program krenuo :'|| w_systime||' sa paramterima: '||w_datum,
                             'FaxServer@esb.hr;', '' ,'text/plain', '', 'text/plain', '');


     BEGIN
        SELECT FIK_IZNOSI+1 INTO W_ZAPREG FROM FIKSNO WHERE FIK_OPISIF = 'ZAPREG';
     EXCEPTION
        WHEN NO_DATA_FOUND THEN
           w_zapreg:=0;
     END;

     -------------------------------------------------------------------------------------------------
     -- Definiranje naziva datoteke, inicijalizacija DEPHDR-a i sl.
     -------------------------------------------------------------------------------------------------
     g_dodatak   := userenv('sessionid');
     -------------------------------------------------------------------------------------------------
     -- Kreiranje naziva datoteke za FINU - ista ne smije imati DEPHDR i u njoj se mora rucno raditi
     -- konverzija znakova na windows 1250 kodnu stranicu
     -------------------------------------------------------------------------------------------------
     g_name_r_dat := 'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'.BNK';
     g_r_dat    := utl_file.fopen(g_oracle_dir_dat,g_name_r_dat,'W');

     g_name_csv_rep := 'POSLANO_'||'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'_'||g_dodatak||'.csv';
     g_csv_rep  := utl_file.fopen(g_oracle_dir_rep,g_name_csv_rep,'W');


     g_name_csv_err := 'GRESKE_'||'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'_'||g_dodatak||'.csv';
     g_csv_err  := utl_file.fopen(g_oracle_dir_err,g_name_csv_err,'W');

     -------------------------------------------------------------------------------------------------
     -- Definiranje log datoteke, inicijalizacija DEPHDR-a i sl.
     -------------------------------------------------------------------------------------------------

     g_path_csv_rep  := 'R_DATOTEKA/' || w_datum || '/' || TO_CHAR(dbms_random.value(1, 100000000), 'FM00000000', 'NLS_NUMERIC_CHARACTERS = '',.''') || '/';
     -------------------------------------------------------------------------------------------------
     utl_file.put_line(g_csv_rep, '$DEPHDR$/' || g_path_csv_rep || g_name_csv_rep);
     utl_file.put_line(g_csv_rep,'SLANJE U JRIR ZA DATUM:'||w_datum_ispis);
     utl_file.put_line(g_csv_rep,';');
     utl_file.put_line(g_csv_rep, 'JMBG;PARTIJA;POS;MATBRO;OIB;NAZIV;NAMJ.STARA;NAMJ.NOVA;BLOK.STARA;BLOK.NOVA;TIP PRIJENOSA;VAZI_DO;INICIJALI KORISNIKA;SEK;CEN;SLU;ORG;TIM');

     -------------------------------------------------------------------------------------------------
     -- Definiranje datoteke rezultata slanja koja sluzi kao kontrola
     -------------------------------------------------------------------------------------------------
     g_path_csv_err  := 'R_DATOTEKA/' || w_datum || '/' || TO_CHAR(dbms_random.value(1, 100000000), 'FM00000000', 'NLS_NUMERIC_CHARACTERS = '',.''') || '/';
     -------------------------------------------------------------------------------------------------
     utl_file.put_line(g_csv_err, '$DEPHDR$/' || g_path_csv_err || g_name_csv_err);
     utl_file.put_line(g_csv_err,'Partija;MB klijenta;Ima grešku;Opis greške;Dat.Pro;Inicijali');
    -------------------------------------------------------------------------------------------------
     riv_slog:=rpad('RIV'||w_gen_depins||w_datum_ispis||'-'||w_vrijeme_ispis||'7777779'||'R2402'||w_datum||lpad(W_ZAPREG,5,0),399,' ')||'V';
     utl_file.put_line(g_r_dat,riv_slog);


     if c_accnt_record%ISOPEN then
        close c_accnt_record;
     end if;

     open c_accnt_record;
     loop
        fetch c_accnt_record bulk collect into tt_account_details limit 500;
        begin
           forall i in indices of tt_account_details save exceptions
              update partija set par_sinkro = 5 where par_sifpar = tt_account_details(i).p_broj_racuna;
              commit;
        exception
           when OTHERS then
              for i in 1..sql%bulk_exceptions.count
              loop
                 common.zapisi_log('Exception Updating Par_sinkro Partija '
                        , sql%bulk_exceptions(i).error_code
                        , dbms_utility.format_error_stack
                        , sqlerrm(-sql%bulk_exceptions(i).error_code)||dbms_utility.format_error_backtrace
                        , 'R_DATOTEKA.P_SALJI_R' );
              end loop;
        end;

        for i in 1..tt_account_details.count
        loop
           begin
              w_cnt_all := w_cnt_all +1;
              w_control_err:=0;
              w_control_override:=0;
              w_descrip_err:='';
              w_novi_racun:=0;

              select count(*) into w_novi_racun from registar_racuna where broj_racuna like '%2402006'||tt_account_details(i).p_broj_racuna;

              select count(*) into w_cnt_potpisnika from POTPISNIK WHERE PTP_NKSPAR = tt_account_details(i).p_broj_racuna AND PTP_TIPOVL = 0 AND PTP_VAZIDO IN(0,99999999);

              if w_cnt_potpisnika = 0 then
                 w_control_override :=1;
                 w_control_err:=1;
                 w_descrip_err:='NISU UNESENI POTPISNICI';
              end if;

              if trim(tt_account_details(i).p_naziv) is null then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNAT NAZIV';
              end if;

              if trim(tt_account_details(i).p_mjesto) is null then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATO MJESTO';
              end if;

              if trim(tt_account_details(i).p_adresa) is null then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATA ADRESA';
              end if;


              if tt_account_details(i).p_opcina = 0 then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATA OPĆINA';
              end if;

              if tt_account_details(i).p_zupanija = 0 then
                 w_control_override :=0;
                 w_control_err:=1;
                 w_descrip_err:='NEPOZNATA ŽUPANIJA';
              end if;


              if w_control_err > 0 and w_control_override = 0 then
                 w_cnt_err := w_cnt_err +1;
                 --w_cnt_chg := w_cnt_chg + 1;

                 err_slog := tt_account_details(i).p_broj_racuna ||';'||
                             tt_account_details(i).p_maticni_broj||';'||
                                                              ' '||';'||
                                                    w_descrip_err||';'||
                               tt_account_details(i).p_datum_prom||';'||
                               tt_account_details(i).p_inicijali;
                 utl_file.put_line(g_csv_err,err_slog);
                 err_slog:='';

              else
                 if w_control_override = 1 then
                    err_slog := tt_account_details(i).p_broj_racuna ||';'||
                                tt_account_details(i).p_maticni_broj||';'||
                                                              ' '||';'||
                                                     w_descrip_err||';'||
                                tt_account_details(i).p_datum_prom||';'||
                                tt_account_details(i).p_inicijali;
                    utl_file.put_line(g_csv_err,err_slog);
                    err_slog:='';
                 end if;
                 ris_slog:=rpad('RIS'||w_gen_bicadr||w_gen_depins
                                     ||tt_account_details(i).p_iban||lpad(tt_account_details(i).p_maticni_broj,8,0)
                                     ||lpad(tt_account_details(i).p_oib,11,0)||rpad(tt_account_details(i).p_naziv,70,' ')
                                     ||lpad(tt_account_details(i).p_zupanija,2,0)||lpad(tt_account_details(i).p_opcina,4,0)
                                     ||rpad(tt_account_details(i).p_adresa,70,' ')||rpad(tt_account_details(i).p_mjesto,70,' ')
                                     ||rpad(tt_account_details(i).p_vazi_od,10,' ')||rpad(tt_account_details(i).p_vazi_do,10,' ')
                                     ||tt_account_details(i).p_rac_prip
                                     ||lpad(case when nvl(tt_account_details(i).p_izv_tip,'0') = '8' then ' ' else  lpad(case when nvl(tt_account_details(i).p_izv_mje,0) = 0 then ' ' else to_char(tt_account_details(i).p_izv_mje) end,5,' ') end,5,' ')
                                     ||tt_account_details(i).p_izv_tip||tt_account_details(i).p_vipkli
                                     ||lpad(case when nvl(tt_account_details(i).p_pjfine,0) = 0 then ' ' else to_char(tt_account_details(i).p_pjfine) end ,5,' ')
                                     ||rpad(case when trim(tt_account_details(i).p_slijednik) = '0' then ' ' else trim(tt_account_details(i).p_slijednik) end ,21,' ')
                                    ,399,' ')||'S';
                 utl_file.put_line(g_r_dat,ris_slog);
                 w_cnt_rec := w_cnt_rec+1;
                 w_cnt_rec_file := w_cnt_rec_file + 1;

                 --SVAKIH 29000 SLOGOVA SE KREIRA NOVA INCIJALNA DADOTEKA
                 IF w_cnt_rec_file = 29000 then
                    SELECT to_char(SYSDATE,'DD.MM.YYYY HH24:mi:ss'), TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')),  to_char(systimestamp,'hh24.mi.ss.FF')
                       INTO w_systime, w_sysdate, w_vrijeme_ispis
                    FROM DUAL;
                    --ZAPIS ZADNJEG SLOGA U DATOTECI
                    riz_slog:=rpad('RIZ'||w_gen_depins||w_datum_ispis||'-'||w_vrijeme_ispis||lpad(w_cnt_rec_file+2,6,0),399,' ')||'Z';
                    utl_file.put_line(g_r_dat,riz_slog);
                    riz_slog:=rpad(' ',399,' ')||'P';
                    utl_file.put_line(g_r_dat,riz_slog);
                    --ZATVARANJE DADOTEKE
                    if utl_file.is_open(g_r_dat) then
                       utl_file.fclose(g_r_dat);
                    end if;
                    --nuliranje brojaca za broj slogova u datoteci i povecavanje brojaca dadoteka iz tablice  FIKSNO
                    w_cnt_rec_file:=0;
                    W_ZAPREG :=W_ZAPREG+1;
                    --OTVARANJE NOVE INCIJALNE DADOTEKE
                    g_name_r_dat := 'R2402'||w_datum||lpad(W_ZAPREG,5,0)||'.BNK';
                    g_r_dat := utl_file.fopen(g_oracle_dir_dat,g_name_r_dat,'W');
                    --SETIRANJE NOVIH VREMENA ZA NOVI RIV SLOG
                    SELECT to_char(SYSDATE,'DD.MM.YYYY HH24:mi:ss'), TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')), to_char(sysdate,'YYYY-MM-DD'), to_char(systimestamp,'hh24.mi.ss.FF')
                      INTO w_systime, w_sysdate, w_datum_ispis, w_vrijeme_ispis
                    FROM DUAL;
                    --ZAPIS PRVOG (RIV) SLOGA U NOVOJ DADOTECI
                    riv_slog:=rpad('RIV'||w_gen_depins||w_datum_ispis||'-'||w_vrijeme_ispis||'7777779'||'R2402'||w_datum||lpad(W_ZAPREG,5,0),399,' ')||'V';
                    utl_file.put_line(g_r_dat,riv_slog);
                 END IF;

                 if w_novi_racun > 0 then
                    w_cnt_chg := w_cnt_chg + 1;
                 else
                    w_cnt_new := w_cnt_new + 1;
                 end if;
                 ris_slog:=' ';
                 rep_slog:=(tt_account_details(i).p_matupr||';'||tt_account_details(i).p_broj_racuna||';'||
                            tt_account_details(i).p_posjed||';'||tt_account_details(i).p_maticni_broj||';'||
                            tt_account_details(i).p_oib||';'||tt_account_details(i).p_naziv||';'||
                            ' '||';'||
                            case when tt_account_details(i).p_namjen = 1 then 'NAMIRENJE'
                                 else 'OBIČAN'
                            end ||';'||
                            case when tt_account_details(i).p_sporni = 2 then 'BLOKIRAN'
                                 else 'AKTIVAN'
                            end ||';'||
                            case when tt_account_details(i).p_sporni = 2 then 'BLOKIRAN'
                                 else 'AKTIVAN'
                            end ||';'||
                            case when tt_account_details(i).p_par_vazido > 0 then
                                    'ZATVARANJE'
                                 else
                                    case when w_novi_racun = 0 then
                                         'OTVARANJE'
                                       else
                                         'PROMJENA'
                                    end
                            end ||';'||
                            tt_account_details(i).p_vazi_do||';'||tt_account_details(i).p_inicijali||';'||
                            tt_account_details(i).p_sektor||';'||tt_account_details(i).p_centar||';'||
                            tt_account_details(i).p_sluzba||';'||tt_account_details(i).p_orgjed||';'||
                            tt_account_details(i).p_siftim||';'
                            );
                 utl_file.put_line(g_csv_rep,rep_slog);
                 rep_slog:='';
              end if;
           end;
        end loop;

        begin
           forall i in indices of tt_account_details save exceptions
              update partija set PAR_SINKRO = 8
              where par_sifpar = tt_account_details(i).p_broj_racuna;
        exception
           when OTHERS then
              for i in 1..sql%bulk_exceptions.count
              loop
                 common.zapisi_log('Exception while Updating Partija Table-' || tt_account_details(i).p_broj_racuna
                                  , sql%bulk_exceptions(i).error_code
                                  , dbms_utility.format_error_stack
                                  , sqlerrm(-sql%bulk_exceptions(i).error_code)||dbms_utility.format_error_backtrace
                                 , 'R_DATOTEKA.P_SALJI_R' );
              end loop;
        end;

        --rollback;
        commit;

        exit when tt_account_details.count < 500;
     END LOOP;

     close c_accnt_record;

     BEGIN
        UPDATE FIKSNO SET FIK_IZNOSI = W_ZAPREG  WHERE FIK_OPISIF = 'ZAPREG';
        --ROLLBACK;
        COMMIT;
     EXCEPTION
        WHEN OTHERS THEN
           w_zapreg:=0;
     END;

     SELECT to_char(SYSDATE,'DD.MM.YYYY HH24:mi:ss'), TO_NUMBER(TO_CHAR(SYSDATE,'YYYYMMDD')),  to_char(systimestamp,'hh24.mi.ss.FF')
       INTO w_systime, w_sysdate, w_vrijeme_ispis
     FROM DUAL;

     riz_slog:=rpad('RIZ'||w_gen_depins||w_datum_ispis||'-'||w_vrijeme_ispis||lpad(w_cnt_rec_file+2,6,0),399,' ')||'Z';
     utl_file.put_line(g_r_dat,riz_slog);
     riz_slog:=rpad(' ',399,' ')||'P';
     utl_file.put_line(g_r_dat,riz_slog);

     utl_file.put_line(g_csv_err,' ');
     utl_file.put_line(g_csv_err,' ');
     err_slog:='POSLANIH: '||w_cnt_rec;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='PROMJENA: '||w_cnt_chg;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='NOVIH: '||w_cnt_new;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='GRESAKA: '||w_cnt_err;
     utl_file.put_line(g_csv_err,err_slog);
     err_slog:='UKUPNO: '||w_cnt_all;
     utl_file.put_line(g_csv_err,err_slog);

     if utl_file.is_open(g_r_dat) then
        utl_file.fclose(g_r_dat);
     end if;

     if utl_file.is_open(g_csv_err) then
        utl_file.fclose(g_csv_err);
     end if;

     if utl_file.is_open(g_csv_rep) then
        utl_file.fclose(g_csv_rep);
     end if;

     --------------------------------------------------------------------------------------------------------------------
     --SLANJE MAILOVA OBAVJESTI S LISTAMA PRIJENOSA I LISTAMA GRESAKA I MAIL O KRAJU PROGRAMA
     --------------------------------------------------------------------------------------------------------------------
     common.utils.depcon_dephdr('PL_OUT',g_name_csv_rep);
     COMMON.UTILS.SEND_EMAIL('(EBC_'||g_okolje||') INICIJALNA R DATOTEKA - lista prijenosa' ,  --subject
                              g_mail_to , --mailto
                              chr(13)||chr(10)||'Postovani/a izvjestaj se nalazi na linku: '||common.utils.getsettingsbyname('DEPHDR_PATH')|| REPLACE(g_path_csv_rep, '/', '\') ||chr(13)||chr(10)||
                              chr(13)||chr(10)||'Molimo Vas da tabelu snimite lokalno kod sebe jer ce ista biti obrisana u roku od tjedan dana. Hvala!' ,  --mailbody
                              'FaxServer@esb.hr',  --mailfrom
                              '' ,
                              'text/plain',
                              '',
                              'text/plain',
                              '' ) ;

     common.utils.depcon_dephdr('PL_OUT', g_name_csv_err);
     COMMON.UTILS.SEND_EMAIL('(EBC_'||g_okolje||') INICIJALNA R DATOTEKA - greske' ,  --subject
                              g_mail_to , --mailto
                              chr(13)||chr(10)||'Postovani/a izvjestaj se nalazi na linku: '||common.utils.getsettingsbyname('DEPHDR_PATH')|| REPLACE(g_path_csv_err, '/', '\') ||chr(13)||chr(10)||
                              chr(13)||chr(10)||'Molimo Vas da tabelu snimite lokalno kod sebe jer ce ista biti obrisana u roku od tjedan dana. Hvala!' ,  --mailbody
                              'FaxServer@esb.hr',  --mailfrom
                              '' ,
                              'text/plain',
                              '',
                              'text/plain',
                              '' ) ;

     common.utils.send_email('(EBC_'||g_okolje||') EOT '||gc_module_name||'.'||c_action_name ,
                              g_mail_err ,
                              'Program zavrsio :'||w_systime ||' sa paramterima: '||w_datum,
                              'FaxServer@esb.hr;', '' ,'text/plain', '', 'text/plain', '');

  EXCEPTION
     WHEN OTHERS THEN
        o_status := 42; --greska u programu, javi TWSu da je otislo kjarcu
        common.zapisi_log(i_comment  => c_action_name,
                          i_sqlcode  => sqlcode,
                          i_sqlerror => DBMS_UTILITY.FORMAT_ERROR_STACK,
                          i_errstack => DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                          i_imepro   => gc_module_name);
        common.utils.send_email('(EBC_'||g_okolje||') ERROR '||gc_module_name||'.'||c_action_name ,
                                  g_mail_err ,
                                  'Greska u  '||gc_module_name||'.'||c_action_name||'. Tip greske: '||sqlerrm||' : '||DBMS_UTILITY.FORMAT_ERROR_BACKTRACE,
                                  'FaxServer@esb.hr;', '' ,'text/plain', '', 'text/plain', '');

        if utl_file.is_open(g_r_dat) then
           utl_file.fclose(g_r_dat);
        end if;

        if utl_file.is_open(g_csv_err) then
           utl_file.fclose(g_csv_err);
        end if;

        if utl_file.is_open(g_csv_rep) then
           utl_file.fclose(g_csv_rep);
        end if;

  END p_salji_r_init;
*/

END R_DATOTEKA;

