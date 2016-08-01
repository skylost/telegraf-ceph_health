#!/usr/bin/python
import rados, sys
import json

def get_ceph_connect():
    try:
        cluster = rados.Rados(conffile = '/etc/ceph/ceph.conf', conf = dict (keyring = '/etc/ceph/ceph.client.admin.keyring'))
        cluster.connect()
    except TypeError as e:
        print 'Argument validation error: ', e
        raise e
    return cluster

def get_ceph_status(cluster):
    status = cluster.mon_command(json.dumps(dict(format='json', prefix='status'), sort_keys=True), '', timeout=10)
    #return status[1]
    return json.loads(status[1])

def get_telegraf_measurement(json_health):
    if json_health['health']['overall_status'] == 'HEALTH_OK':
        health = "0"
    elif json_health['health']['overall_status'] == 'HEALTH_WARN':
        health = "1"
    else:
        health = "2"

    health_summary="NA"
    for summary in json_health['health']['summary']:
        if summary['summary']:
            health_summary = json_health['health']['summary'][0]['summary']
    fsid = json_health['fsid']
    num_osds = str(json_health['osdmap']['osdmap']['num_osds'])
    num_osds_up = str(json_health['osdmap']['osdmap']['num_up_osds'])
    num_osds_in = str(json_health['osdmap']['osdmap']['num_in_osds'])
    num_pgs = str(json_health['pgmap']['num_pgs'])
    num_pgs_remapped = str(json_health['osdmap']['osdmap']['num_remapped_pgs'])
    mds_in = str(json_health['mdsmap']['in'])
    mds_up = str(json_health['mdsmap']['up'])
    mds_max = str(json_health['mdsmap']['max'])
    bytes_avail = str(json_health['pgmap']['bytes_avail'])
    bytes_total = str(json_health['pgmap']['bytes_total'])
    bytes_used = str(json_health['pgmap']['bytes_used'])
    data_bytes = str(json_health['pgmap']['data_bytes'])

    measurement = ('ceph_health' + ',health_summary=' + health_summary.replace(',','_').replace(' ','-') + ',fsid=' + fsid + " health=" + health + ',' 
                  + 'num_osds=' + num_osds + ',' + 'num_osds_up=' + num_osds_up + ',' 
                  + 'num_osds_in=' + num_osds_in + ',' + 'num_pgs=' + num_pgs + ',' 
                  + 'num_pgs_remapped=' + num_pgs_remapped + ',' + 'mds_in=' + mds_in + ',' 
                  + 'mds_up=' + mds_up + ',' + 'mds_max=' + mds_max + ',' 
                  + 'bytes_avail=' + bytes_avail + ',' + 'bytes_total=' + bytes_total + ','
                  + 'bytes_used=' + bytes_used + ',' + 'data_bytes=' + data_bytes
                  )
    print measurement

def main():
    cluster=get_ceph_connect()
    get_telegraf_measurement(get_ceph_status(cluster))
    cluster.shutdown()

if __name__ == "__main__":
    main()
