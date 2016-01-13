//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package com.cloud.hypervisor.kvm.resource.wrapper;

import com.cloud.agent.api.*;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.snapshot.VMSnapshot;
import org.apache.log4j.Logger;
import org.libvirt.Connect;
import org.libvirt.Domain;
import org.libvirt.LibvirtException;
import com.cloud.hypervisor.kvm.resource.LibvirtVMDef.DiskDef;

@ResourceWrapper(handles =  CreateVMSnapshotCommand.class)
public final class LibvirtCreateVMSnapshotCommandWrapper extends CommandWrapper<CreateVMSnapshotCommand, Answer, LibvirtComputingResource> {

    private static final Logger s_logger = Logger.getLogger(LibvirtCreateVMSnapshotCommandWrapper.class);

    @Override
    public Answer execute(final CreateVMSnapshotCommand command, final LibvirtComputingResource libvirtComputingResource) {
        final String vmName = command.getVmName();

        final StringBuilder xmlSnapshot = new StringBuilder();

        try {
            final LibvirtUtilitiesHelper libvirtUtilitiesHelper = libvirtComputingResource.getLibvirtUtilitiesHelper();
            final Connect conn = libvirtUtilitiesHelper.getConnectionByVmName(vmName);

            Domain vm = null;
            if (vmName != null) {
                try {
                    vm = libvirtComputingResource.getDomain(conn, command.getVmName());
                } catch (final LibvirtException e) {
                    s_logger.trace("Ignoring libvirt error.", e);
                }
            }

            xmlSnapshot.append("<domainsnapshot>");

            xmlSnapshot.append("<name>" + command.getTarget().getSnapshotName() + "</name>");

            if(command.getTarget().getType() == VMSnapshot.Type.DiskAndMemory)
                xmlSnapshot.append("<memory snapshot='internal' />");
            else
                xmlSnapshot.append("<memory snapshot='none' />");

            xmlSnapshot.append("<disks>");
            for(DiskDef disk : libvirtComputingResource.getDisks(conn, vm.getName())){
                if(disk.getDeviceType() == DiskDef.DeviceType.DISK) {
                    if(disk.getDiskProtocol() == DiskDef.DiskProtocol.RBD)
                        return new CreateVMSnapshotAnswer(command, false, "At the moment there is no support for CEPH(RBD) snapshotting");
                    else
                        xmlSnapshot.append("<disk snapshot='internal' name='" + disk.getDiskLabel() + "' />");
                }
            }
            xmlSnapshot.append("</disks>");
            xmlSnapshot.append("</domainsnapshot>");

            vm.snapshotCreateXML(xmlSnapshot.toString());

        } catch (final LibvirtException e) {
            s_logger.error("Something went wrong while trying to make the snapshot with xml: " + xmlSnapshot.toString());
            return new CreateVMSnapshotAnswer(command, false, e.toString());
        } catch (final CloudRuntimeException e) {
            return new CreateVMSnapshotAnswer(command, false, e.toString());
        }

        return new CreateVMSnapshotAnswer(command, command.getTarget(), command.getVolumeTOs());
    }
}
