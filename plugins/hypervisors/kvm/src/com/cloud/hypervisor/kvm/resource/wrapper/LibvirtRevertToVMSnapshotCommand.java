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

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.RevertToVMSnapshotAnswer;
import com.cloud.agent.api.RevertToVMSnapshotCommand;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.hypervisor.kvm.resource.LibvirtVMDef.DiskDef;
import com.cloud.hypervisor.kvm.resource.LibvirtVMSnapshotDef;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.exception.CloudRuntimeException;
import com.cloud.vm.VirtualMachine.PowerState;
import com.cloud.vm.snapshot.VMSnapshot;
import org.apache.log4j.Logger;
import org.libvirt.Connect;
import org.libvirt.Domain;
import org.libvirt.DomainSnapshot;
import org.libvirt.LibvirtException;

@ResourceWrapper(handles = RevertToVMSnapshotCommand.class)
public final class LibvirtRevertToVMSnapshotCommand extends CommandWrapper<RevertToVMSnapshotCommand, Answer, LibvirtComputingResource> {

    private static final Logger s_logger = Logger.getLogger(LibvirtRevertToVMSnapshotCommand.class);

    @Override
    public Answer execute(final RevertToVMSnapshotCommand command, final LibvirtComputingResource libvirtComputingResource) {
        final String vmName = command.getVmName();
        final String snapshotName = command.getTarget().getSnapshotName();
        final boolean onlineSnapshot = command.getTarget().getType() == VMSnapshot.Type.DiskAndMemory;

        PowerState vmState = null;

        final StringBuilder xmlSnapshot = new StringBuilder();

        try {
            final LibvirtUtilitiesHelper libvirtUtilitiesHelper = libvirtComputingResource.getLibvirtUtilitiesHelper();
            final Connect conn = libvirtUtilitiesHelper.getConnectionByVmName(vmName);

            Domain vm = null;
            if (vmName != null) {
                try {
                    vm = libvirtComputingResource.getDomain(conn, command.getVmName());
                } catch (final LibvirtException e) {
                    s_logger.trace("VM not found, cannot revert to snapshot", e);
                    return new RevertToVMSnapshotAnswer(command, false, "VM not found, cannot revert to snapshot");
                }
            }

            DomainSnapshot snapshot = null;

            try {
                 snapshot = vm.snapshotLookupByName(snapshotName);
            } catch (final LibvirtException e) {
                if(s_logger.isDebugEnabled())
                    s_logger.debug("VM snapshot not found, trying to restore the snapshot in libvirt");
            }

            /**
             * When a VM is destroyed on a host Libvirt loses all info on Snapshots
             */

            if (snapshot == null){
                // Snapshot is not found, most likely because this vm has been powered off and/or has been moved to another host.

                try {
                    LibvirtVMSnapshotDef vmSnapshotDef = new LibvirtVMSnapshotDef(vm, command.getTarget(), command.getTarget().getType(), false);

                    for (DiskDef disk : libvirtComputingResource.getDisks(conn, vm.getName()))
                        try {
                            vmSnapshotDef.addDisk(disk);
                        } catch (LibvirtVMSnapshotDef.UnsupportedDiskException e) {
                            s_logger.error(e.toString());
                            return new RevertToVMSnapshotAnswer(command, false, e.toString());
                        }

                    snapshot = vm.snapshotCreateXML(vmSnapshotDef.toString(), vmSnapshotDef.getFlags());
                } catch(LibvirtException e){
                    s_logger.error("Unable to recreate VM snapshot-definition: \n" + e.toString());
                    return new RevertToVMSnapshotAnswer(command, false, "Unable to recreate VM snapshot-definition");
                }
            }

            if(snapshot != null)
                vm.revertToSnapshot(snapshot);
            else
                return new RevertToVMSnapshotAnswer(command, false, (new CloudRuntimeException("Can't revert to the requested snapshot as it does not exist!")).toString());

            if(command.getTarget().getType() != VMSnapshot.Type.DiskAndMemory) {
                vm.destroy();
                vmState = PowerState.PowerOff;
            } else
                vmState = PowerState.PowerOn;

        } catch (final LibvirtException e) {
            return new RevertToVMSnapshotAnswer(command, false, e.toString());
        } catch (final CloudRuntimeException e) {
            return new RevertToVMSnapshotAnswer(command, false, e.toString());
        }

        return new RevertToVMSnapshotAnswer(command, command.getVolumeTOs(), vmState);
    }
}
