package com.cloud.hypervisor.kvm.resource;

import com.cloud.agent.api.VMSnapshotTO;
import com.cloud.vm.snapshot.VMSnapshot.Type;
import com.cloud.hypervisor.kvm.resource.LibvirtVMDef.DiskDef;
import org.apache.log4j.Logger;
import org.libvirt.Domain;
import org.libvirt.LibvirtException;

import java.util.ArrayList;
import java.util.List;

public class LibvirtVMSnapshotDef {

    private static final Logger s_logger = Logger.getLogger(LibvirtVMSnapshotDef.class);

    private Domain _domain;
    private boolean _isNewSnapshot;
    private Type _snapshotType;
    private VMSnapshotTO _vmSnapshot;
    private int _flags;

    private String _xmlDesc;

    private final List<DiskDef> disks = new ArrayList<>();

    public LibvirtVMSnapshotDef(Domain domain, VMSnapshotTO vmSnapshot, Type snapshotType, boolean isNewSnapshot) throws LibvirtException{
        _domain = domain;
        _vmSnapshot = vmSnapshot;
        _snapshotType = snapshotType;
        _isNewSnapshot = isNewSnapshot;

        if(isNewSnapshot) {
            _flags = 1; // VIR_DOMAIN_SNAPSHOT_CREATE_CURRENT
            if(_vmSnapshot.getCurrent())
                _flags |= 2; // VIR_DOMAIN_SNAPSHOT_CREATE_REDEFINE

        } else {
            _flags = 0;
        }

        try {
            _xmlDesc = domain.getXMLDesc(0);
        } catch(LibvirtException e) {
            s_logger.error("Could not retrieve XML from libvirt for snapshot: " + _vmSnapshot.getSnapshotName());
            throw e;
        }
    }

    public void addDisk(DiskDef diskDef) throws UnsupportedDiskException {
        if(diskDef.getDiskProtocol() == LibvirtVMDef.DiskDef.DiskProtocol.RBD)
            throw new UnsupportedDiskException("At the moment there is no support for CEPH(RBD) snapshotting");

        disks.add(diskDef);
    }

    public int getFlags(){
        return _flags;
    }

    @Override
    public String toString(){
        StringBuilder xmlSnapshot = new StringBuilder();

        xmlSnapshot.append("<domainsnapshot>");

        xmlSnapshot.append("<name>" + _vmSnapshot.getSnapshotName() + "</name>");

        if(!_isNewSnapshot) { // Only when we try to recreate snapshotxml we should run this
            xmlSnapshot.append("<creationTime>" + _vmSnapshot.getCreateTime().longValue() + "</creationTime>");

            if(_vmSnapshot.getType() == Type.DiskAndMemory)
                xmlSnapshot.append("<state>running</state>\n");
            else
                xmlSnapshot.append("<state>stopped</state>\n");
        }

        if(_vmSnapshot.getType() == Type.DiskAndMemory)
            xmlSnapshot.append("<memory snapshot='internal' />");
        else
            xmlSnapshot.append("<memory snapshot='none' />");

        xmlSnapshot.append("<disks>");
        for(LibvirtVMDef.DiskDef disk : disks){
            if(disk.getDeviceType() == LibvirtVMDef.DiskDef.DeviceType.DISK) {
                xmlSnapshot.append("<disk snapshot='internal' name='" + disk.getDiskLabel() + "' />");
            }
        }
        xmlSnapshot.append("</disks>");

        if(!_isNewSnapshot)
            xmlSnapshot.append(_xmlDesc);

        xmlSnapshot.append("</domainsnapshot>");

        return xmlSnapshot.toString();
    }

    public static class UnsupportedDiskException extends Exception {
        public UnsupportedDiskException(String e){
            super(e);
        }
    }

}
