from prophecy_pipeline_sdk.graph import *
from prophecy_pipeline_sdk.properties import *
Schedules = [Schedule(
               Name = "Schedule 1",
               emails = ["email@gmail.com"],
               enabled = False,
               cron = "* 0 2 * * * *",
               timezone = "GMT"
             )]
args = PipelineArgs(
    label = "C0030029_Aprovisionamiento_Dynatech",
    version = 1,
    auto_layout = False,
    params = Parameters(ref_data_date_part = 20260331),
    schedules = Schedules
)

with Pipeline(args) as pipeline:

    with visual_group("HistoricoMovimientos"):
        c0030029_aprovisionamiento_dynatech__countrecords_1 = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__CountRecords_1",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__CountRecords_1")
        )
        c0030029_aprovisionamiento_dynatech__countrecords_2 = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__CountRecords_2",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__CountRecords_2")
        )
        c0030029_aprovisionamiento_dynatech__formula_360_0 = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__Formula_360_0",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__Formula_360_0")
        )
        c0030029_aprovisionamiento_dynatech__join_349_inner = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__Join_349_inner",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__Join_349_inner"),
            input_ports = ["in_0", "in_1"]
        )
        c0030029_aprovisionamiento_dynatech__productos_inversiones_transacciones = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_transacciones",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_transacciones"),
            input_ports = ["in_0", "in_1", "in_2"]
        )

    with visual_group("HistoricodePrecios"):
        c0030029_aprovisionamiento_dynatech__productos_inversiones_precios = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_precios",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_precios")
        )

    with visual_group("Inventario"):
        c0030029_aprovisionamiento_dynatech__productos_inversiones_inventario = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_inventario",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_inventario"),
            input_ports = ["in_0", "in_1", "in_2"]
        )

    with visual_group("TablaContratos"):
        c0030029_aprovisionamiento_dynatech__productos_inversiones_contratos = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_contratos",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_contratos"),
            input_ports = ["in_0", "in_1"]
        )

    with visual_group("TablaValores"):
        c0030029_aprovisionamiento_dynatech__productos_inversiones_valores = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_valores",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_valores")
        )

    with visual_group("Tabla_cuentas_cash_asociadas_a_contratos_dynatech"):
        c0030029_aprovisionamiento_dynatech__filter_30 = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__Filter_30",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__Filter_30")
        )
        c0030029_aprovisionamiento_dynatech__productos_inversiones_relacion_contratos_cuentas = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_relacion_contratos_cuentas",
            properties = ModelTransform(
              modelName = "C0030029_Aprovisionamiento_Dynatech__productos_inversiones_relacion_contratos_cuentas"
            )
        )

    with visual_group("Tablapersonasparticipantesdeloscontratos"):
        c0030029_aprovisionamiento_dynatech__alteryxselect_65 = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__AlteryxSelect_65"),
            input_ports = ["in_0", "in_1", "in_2"]
        )
        c0030029_aprovisionamiento_dynatech__cast_productos_inversiones_personas = Process(
            name = "C0030029_Aprovisionamiento_Dynatech__cast_productos_inversiones_personas",
            properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__cast_productos_inversiones_personas")
        )

    c0030029_aprovisionamiento_dynatech__countrecords_3 = Process(
        name = "C0030029_Aprovisionamiento_Dynatech__CountRecords_3",
        properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__CountRecords_3")
    )
    c0030029_aprovisionamiento_dynatech__countrecords_4 = Process(
        name = "C0030029_Aprovisionamiento_Dynatech__CountRecords_4",
        properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__CountRecords_4")
    )
    c0030029_aprovisionamiento_dynatech__countrecords_5 = Process(
        name = "C0030029_Aprovisionamiento_Dynatech__CountRecords_5",
        properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__CountRecords_5")
    )
    c0030029_aprovisionamiento_dynatech__primary_holder_count = Process(
        name = "C0030029_Aprovisionamiento_Dynatech__primary_holder_count",
        properties = ModelTransform(modelName = "C0030029_Aprovisionamiento_Dynatech__primary_holder_count")
    )
    dbo_dynclientescuentasapp = Process(
        name = "dbo_dynclientescuentasapp",
        properties = Dataset(
          table = Dataset.DBTSource(name = "dbo_dynclientescuentasapp", sourceType = "Table", sourceName = "ayush_demos_demos"),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        comment = "Loads client account data from the ayush_demos_demos source for further processing."
    )
    dbo_dynclientestitularesapp = Process(
        name = "dbo_dynclientestitularesapp",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "dbo_dynclientestitularesapp",
            sourceType = "Table",
            sourceName = "ayush_demos_demos"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        comment = "Loads client and account holder data from the ayush_demos_demos database for further processing."
    )
    productos_cuentas_inventario = Process(
        name = "productos_cuentas_inventario",
        properties = Dataset(
          table = Dataset.DBTSource(
            name = "productos_cuentas_inventario",
            sourceType = "Table",
            sourceName = "ayush_demos_demos"
          ),
          writeOptions = {"writeMode" : "overwrite"}
        ),
        comment = "Overwrites the inventory accounts product data for business reporting."
    )
    (
        c0030029_aprovisionamiento_dynatech__alteryxselect_65._out(0)
        >> [c0030029_aprovisionamiento_dynatech__productos_inversiones_contratos._in(0),
              c0030029_aprovisionamiento_dynatech__productos_inversiones_transacciones._in(0),
              c0030029_aprovisionamiento_dynatech__productos_inversiones_inventario._in(0),
              c0030029_aprovisionamiento_dynatech__countrecords_3._in(0),
              c0030029_aprovisionamiento_dynatech__countrecords_4._in(0),
              c0030029_aprovisionamiento_dynatech__countrecords_5._in(0),
              c0030029_aprovisionamiento_dynatech__cast_productos_inversiones_personas._in(0),
              c0030029_aprovisionamiento_dynatech__primary_holder_count._in(0)]
    )
    (
        c0030029_aprovisionamiento_dynatech__join_349_inner._out(0)
        >> [c0030029_aprovisionamiento_dynatech__productos_inversiones_transacciones._in(1),
              c0030029_aprovisionamiento_dynatech__countrecords_1._in(0)]
    )
    (
        c0030029_aprovisionamiento_dynatech__formula_360_0._out(0)
        >> [c0030029_aprovisionamiento_dynatech__join_349_inner._in(0),
              c0030029_aprovisionamiento_dynatech__countrecords_2._in(0)]
    )
    (
        c0030029_aprovisionamiento_dynatech__filter_30._out(0)
        >> [c0030029_aprovisionamiento_dynatech__alteryxselect_65._in(0),
              c0030029_aprovisionamiento_dynatech__productos_inversiones_relacion_contratos_cuentas._in(0)]
    )
