# Harvester

## todo
- master启动完成后才能进行rpc调用(ensure db),否则grpc 出现 connection is unavailable
- master启动完成（master tasks)后才能启动shipper（否则shipper发来的数据引起grpc调用但server未就绪)

## Search

req(db_name+query) -> Harvesterd -> routing to the db impl, send query -> db
                                                            data       <-