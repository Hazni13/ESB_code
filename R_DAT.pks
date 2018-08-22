create or replace PACKAGE       R_DATOTEKA AS

/*   *
*================================================================================================================<br/>
*   PURPOSE: Paket za kreiranje i prijem R datoteke iz FINAe
*
*   @version 1.0.0
*   @author  Milan Klen
*   @headcom
*
*  REVISIONS:
*  Ver.           Date            Author            Request_ID            Description
*  ================================================================================================================<br/>
*  1.0.0          15.03.2015                                          Inicijalno kreiranje
*/

 type r_account_details is record
  (
   p_broj_racuna   number(18),
   p_iban          varchar(21),
   p_maticni_broj  number(8),
   p_oib           number(11),
   p_naziv         varchar2(70),
   p_zupanija      number(2),
   p_opcina        number(4),
   p_adresa        varchar2(70),
   p_mjesto        varchar2(70),
   p_vazi_od       varchar2(10),
   p_vazi_do       varchar2(10),
   p_rac_prip      number(1),
   p_izv_mje       number(5),
   p_izv_tip       varchar2(1),
   p_vipkli        number(1),
   p_pjfine        number(5),
   p_slijednik     varchar2(21),
   p_datum_prom    date,
   p_inicijali     varchar2(3),
   p_sektor        number(3),
   p_centar        number(4),
   p_sluzba        number(4),
   p_orgjed        number(4),
   p_siftim        number(4),
   p_posjed        number(4),
   p_matupr        number(13),
   p_namjen        number(2),
   p_sporni        number(2),
   p_par_vaziod    date,
   p_par_vazido    date,
   p_par_id        number
  );

  type t_account_details is table of r_account_details;



/*
   ********************************************************************************************<br/>
      PUBLIC GLOBAL PROCEDURES DECLARATIONS
   ********************************************************************************************<br/>
*/

/*
   Name: p_slanje_r
   Purpose: procedura za kreiranje R datoteke i slanje iste u registar
   * @author    NK3
   * @version   1.0
*/

procedure p_salji_r (o_status out number , i_datum number default null);

/*
 Name: p_slanje_r
 Purpose: procedura za kreiranje inicijalne R datoteke i slanje iste u registar
 * @author    NK3
 * @version   1.0


procedure p_salji_r_init (o_status out number , i_datum number default null);*/

END R_DATOTEKA;