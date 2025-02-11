helm upgrade -n wb-cluster alice kubernetes/chart/ --values kubernetes/config/common.yaml --values kubernetes/config/alice.yaml
helm upgrade -n wb-cluster bob kubernetes/chart/ --values kubernetes/config/common.yaml --values kubernetes/config/bob.yaml
helm upgrade -n wb-cluster carl kubernetes/chart/ --values kubernetes/config/common.yaml --values kubernetes/config/carl.yaml
