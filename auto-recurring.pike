#!/usr/local/bin/pike

#define SQ(a)   replace(a, ({ "\"", "'", "\\" }), ({ "\\\"" , "\\'", "\\\\" }))

#include "settings.pike"

#define DEBUG

object db = Sql.Sql("mysql://"+DBHOST,DBUSER,DBBASE,DBPASS);

mapping|int get_contact(array(mapping) factures, mapping societe) {
  mapping|int contact = 0;
  foreach(factures, mapping facture) {
    array(mapping) contacts = db->query("SELECT email,CONCAT(firstname, ' ', lastname) AS nom,address,zip AS cp,town AS ville FROM llx_socpeople WHERE rowid IN (select fk_socpeople FROM llx_element_contact WHERE element_id='"+facture["rowid"]+"');");
    if(!sizeof(contacts)) {
      continue;
    } else {
      contact = contacts[0];
    }
  }
  if(!contact)
    contact = societe;
  return contact;
}

int main(int argc, array(string) argv) {

  array(mapping) societes = db->query("SELECT rowid,email,nom,address,zip as cp,town as ville FROM llx_societe;");

  string news = "";

#ifdef DEBUG
  write("Backing up database.\n");
  Process.popen("mysqldump -h "+DBHOST+" -u "+DBUSER+" -p"+DBPASS+" "+DBBASE+" > ~/backups/"+DBBASE+".`date +%Y%m%d%H%M`.sql");
  sleep(1);
#endif

  write("Populating tracking table...");
  // First, fill the autofac table
  db->query("CREATE TABLE temp2 AS (SELECT facnumber FROM autofac);");
  // fk_statut : 0 = draft, 1 = valid, 2 = paid , 3 = abandoned
  db->query("INSERT INTO autofac(rowid,facnumber) SELECT rowid,facnumber FROM llx_facture WHERE facnumber NOT IN (SELECT facnumber FROM temp2) AND (fk_statut=1 OR fk_statut=2);");
  db->query("DROP TABLE temp2;");
  write(" done.\n");
 
  array recurs = db->query("SELECT rowid,facnumber FROM autofac WHERE status=0 ORDER BY facnumber ASC;"); // check if recurring
  foreach(recurs, mapping recur) {
    array lines = db->query("SELECT date_start,date_end FROM llx_facturedet WHERE fk_facture='"+SQ(recur["rowid"])+"';");
    int nr = 1;
    foreach(lines, mapping line) {
      if(line["date_start"]!=0 || line["date_end"]!=0) {
	nr = 0;
      }
    }
    if(nr) {
      db->query("UPDATE autofac SET status=9 WHERE rowid='"+SQ(recur["rowid"])+"';");
    }
  }

  recurs = db->query("SELECT DISTINCT(fk_facture) AS rowid FROM llx_facturedet \
      WHERE date_end < NOW() + INTERVAL 2 MONTH \
      AND fk_facture IN (SELECT rowid FROM autofac WHERE status=0);"); // process if service expiration is in less than 2 months
  foreach(recurs, mapping recur) {
    // copy content of llx_facture, llx_facturedet and llx_element_contact
    string next = db->query("SELECT MAX(rowid)+1 AS next FROM llx_facture;")[0]["next"];
    string datef = db->query("SELECT (MIN(date_end) - INTERVAL 10 DAY) AS datef FROM llx_facturedet WHERE date_end IS NOT NULL AND fk_facture='"+SQ(recur["rowid"])+"';")[0]["datef"];
    db->query("INSERT INTO llx_facture(rowid,facnumber,entity,ref_ext,ref_int,type,ref_client,increment,fk_soc,datec,datef,date_valid,tms,paye,amount,remise_percent,remise_absolue,remise,close_code,close_note,tva,localtax1,localtax2,revenuestamp,total,total_ttc,fk_statut,fk_user_author,fk_user_valid,fk_facture_source,fk_projet,fk_account,fk_currency,fk_cond_reglement,fk_mode_reglement,date_lim_reglement,note_private,note_public,model_pdf,import_key,extraparams) \
	SELECT '"+SQ(next)+"','(PROV"+SQ(next)+")',entity,ref_ext,ref_int,type,ref_client,increment,fk_soc,now(),'"+SQ(datef)+"','',tms,0,amount,remise_percent,remise_absolue,remise,close_code,close_note,tva,localtax2,localtax2,revenuestamp,total,total_ttc,0,fk_user_author,fk_user_valid,fk_facture_source,fk_projet,fk_account,fk_currency,fk_cond_reglement,fk_mode_reglement,'"+SQ(datef)+"','Auto-generated invoice',note_public,model_pdf,import_key,extraparams FROM llx_facture WHERE rowid='"+SQ(recur["rowid"])+"';");
    db->query("INSERT INTO llx_facturedet(fk_facture,fk_parent_line,fk_product,label,description,tva_tx,localtax1_tx,localtax1_type,localtax2_tx,localtax2_type,qty,remise_percent,remise,fk_remise_except,subprice,price,total_ht,total_tva,total_localtax1,total_localtax2,total_ttc,product_type,date_start,date_end,info_bits,fk_product_fournisseur_price,buy_price_ht,fk_code_ventilation,special_code,rang,import_key) \
	(SELECT '"+SQ(next)+"',fk_parent_line,fk_product,label,description,tva_tx,localtax1_tx,localtax1_type,localtax2_tx,localtax2_type,qty,remise_percent,remise,fk_remise_except,subprice,price,total_ht,total_tva,total_localtax1,total_localtax2,total_ttc,product_type,date(date(date_start)+(date(date_end)-date(date_start))),date(date(date_end)+(date(date_end)-date(date_start))),info_bits,fk_product_fournisseur_price,buy_price_ht,fk_code_ventilation,special_code,rang,import_key FROM llx_facturedet WHERE (date_end IS NOT NULL OR date_start IS NOT NULL) AND fk_facture='"+SQ(recur["rowid"])+"');");
    db->query("INSERT INTO llx_facturedet(fk_facture,fk_parent_line,fk_product,label,description,tva_tx,localtax1_tx,localtax1_type,localtax2_tx,localtax2_type,qty,remise_percent,remise,fk_remise_except,subprice,price,total_ht,total_tva,total_localtax1,total_localtax2,total_ttc,product_type,date_start,date_end,info_bits,fk_product_fournisseur_price,buy_price_ht,fk_code_ventilation,special_code,rang,import_key) \
	(SELECT '"+SQ(next)+"',fk_parent_line,fk_product,label,description,tva_tx,localtax1_tx,localtax1_type,localtax2_tx,localtax2_type,qty,remise_percent,remise,fk_remise_except,subprice,price,total_ht,total_tva,total_localtax1,total_localtax2,total_ttc,product_type,date_start,date_end,info_bits,fk_product_fournisseur_price,buy_price_ht,fk_code_ventilation,special_code,rang,import_key FROM llx_facturedet WHERE (date_end IS NULL AND date_start IS NULL) AND fk_facture='"+SQ(recur["rowid"])+"');");
    db->query("INSERT INTO llx_element_contact(datecreate,statut,element_id,fk_c_type_contact,fk_socpeople) \
	SELECT now(),statut,'"+SQ(next)+"',fk_c_type_contact,fk_socpeople FROM llx_element_contact WHERE statut='4' AND element_id='"+SQ(recur["rowid"])+"';");
    db->query("UPDATE autofac SET status='1' WHERE rowid='"+SQ(recur["rowid"])+"';");
    news += DOLIBASEURL + "compta/facture.php?facid=" + next +"\n";
  }

  write(" Done!\n");
  write("To check :\n%s\n", news);
  return 0;
}
