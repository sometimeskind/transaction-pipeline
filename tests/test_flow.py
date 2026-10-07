from transaction_pipeline.flow import import_flow


def test_flow_is_registered_under_its_deployment_name():
    assert import_flow.name == "transaction-import"
