export const DEFAULT_CIMAVET_ZIP_URL = "https://listadomedicamentos.aemps.gob.es/prescripcionVET.zip";
export const UPSERT_BATCH_SIZE = 100;
export const DEFAULT_FLUSH_TRIGGER = 25;

export const CIMAVET_DICT_CONFIG = {
  "DICCIONARIO_ATC.xml": {
    table: "cimavet_dic_atc",
    listTag: "atc",
    pk: "codigoatc",
    map: {
      codigoatc: { col: "codigoatc", type: "text" },
      descatc: { col: "descatc", type: "text" }
    }
  },
  "DICCIONARIO_LABORATORIOS.xml": {
    table: "cimavet_dic_laboratorios",
    listTag: "laboratorios",
    pk: "nro_labor",
    map: {
      nro_labor: { col: "nro_labor", type: "int" },
      des_labor: { col: "des_labor", type: "text" }
    }
  },
  "DICCIONARIO_PRINCIPIOS_ACTIVOS.xml": {
    table: "cimavet_dic_principios_activos",
    listTag: "principiosactivos",
    pk: "nroprincipioactivo",
    map: {
      nroprincipioactivo: { col: "nroprincipioactivo", type: "int" },
      codigoprincipioactivo: { col: "codigoprincipioactivo", type: "text" },
      principioactivo: { col: "principioactivo", type: "text" }
    }
  },
  "DICCIONARIO_ESPECIES_DESTINO.xml": {
    table: "cimavet_dic_especies_destino",
    listTag: "especiesdestino",
    pk: "cod_espdes",
    map: {
      cod_espdes: { col: "cod_espdes", type: "int" },
      des_espdes: { col: "des_espdes", type: "text" }
    }
  },
  "DICCIONARIO_VIAS_ADMINISTRACION.xml": {
    table: "cimavet_dic_vias_administracion",
    listTag: "viasadministracion",
    pk: "codigoviaadministracion",
    map: {
      codigoviaadministracion: { col: "codigoviaadministracion", type: "int" },
      viaadministracion: { col: "viaadministracion", type: "text" }
    }
  },
  "DICCIONARIO_FORMA_FARMACEUTICA.xml": {
    table: "cimavet_dic_formas_farmaceuticas",
    listTag: "formasfarmaceuticas",
    pk: "codigoformafarmaceutica",
    map: {
      codigoformafarmaceutica: { col: "codigoformafarmaceutica", type: "int" },
      formafarmaceutica: { col: "formafarmaceutica", type: "text" }
    }
  },
  "DICCIONARIO_INDICACIONES.xml": {
    table: "cimavet_dic_indicaciones",
    listTag: "indicaciones",
    pk: "id_indicacion",
    map: {
      id_indicacion: { col: "id_indicacion", type: "int" },
      ds_indicacion: { col: "ds_indicacion", type: "text" }
    }
  },
  "DICCIONARIO_CONTRAINDICICACIONES.xml": {
    table: "cimavet_dic_contraindicaciones",
    listTag: "contraindicaciones",
    pk: "id_contraindicacion",
    map: {
      id_contraindicacion: { col: "id_contraindicacion", type: "int" },
      ds_contraindicacion: { col: "ds_contraindicacion", type: "text" }
    }
  },
  "DICCIONARIO_INTERACCIONES.xml": {
    table: "cimavet_dic_interacciones",
    listTag: "interacciones",
    pk: "id_interaccion",
    map: {
      id_interaccion: { col: "id_interaccion", type: "int" },
      ds_interaccion: { col: "ds_interaccion", type: "text" }
    }
  },
  "DICCIONARIO_REACCIONES_ADVERSAS.xml": {
    table: "cimavet_dic_reacciones_adversas",
    listTag: "reacciones_adversas",
    pk: "id_signo",
    map: {
      id_signo: { col: "id_signo", type: "int" },
      ds_signo: { col: "ds_signo", type: "text" }
    }
  },
  "DICCIONARIO_SITUACION_REGISTRO.xml": {
    table: "cimavet_dic_situacion_registro",
    listTag: "situacionesregistro",
    pk: "codigosituacionregistro",
    map: {
      codigosituacionregistro: { col: "codigosituacionregistro", type: "int" },
      situacionregistro: { col: "situacionregistro", type: "text" }
    }
  },
  "DICCIONARIO_UNIDAD_COMPOSICION.xml": {
    table: "cimavet_dic_unidad_composicion",
    listTag: "unidadescomposicion",
    pk: "codigounidadcomposicion",
    map: {
      codigounidadcomposicion: { col: "codigounidadcomposicion", type: "int" },
      unidadcomposicion: { col: "unidadcomposicion", type: "text" }
    }
  },
  "DICCIONARIO_DISPOSITIVOS.xml": {
    table: "cimavet_dic_dispositivos",
    listTag: "dispositivos",
    pk: "codigodispositivo",
    map: {
      codigodispositivo: { col: "codigodispositivo", type: "int" },
      dispositivo: { col: "dispositivo", type: "text" }
    }
  }
};
