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
import com.cloud.agent.api.DeleteVMSnapshotAnswer;
import com.cloud.agent.api.DeleteVMSnapshotCommand;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.hypervisor.kvm.resource.LibvirtVMDef;
import com.cloud.hypervisor.kvm.resource.LibvirtVMSnapshotDef;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.exception.CloudRuntimeException;
import org.apache.log4j.Logger;
import org.libvirt.Connect;
import org.libvirt.Domain;
import org.libvirt.DomainSnapshot;
import org.libvirt.LibvirtException;

@ResourceWrapper(handles = DeleteVMSnapshotCommand.class)
public final class LibvirtDeleteVMSnapshotCommandWrapper  extends CommandWrapper<DeleteVMSnapshotCommand, Answer, LibvirtComputingResource> {

    private static final Logger s_logger = Logger.getLogger(LibvirtDeleteVMSnapshotCommandWrapper.class);

    @Override
    public Answer execute(final DeleteVMSnapshotCommand command, final LibvirtComputingResource libvirtComputingResource) {
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

            DomainSnapshot snapshot = null;

            try {
                snapshot = vm.snapshotLookupByName(command.getTarget().getSnapshotName());
            } catch(LibvirtException e) {
                if (s_logger.isDebugEnabled())
                    s_logger.debug("Could not find snapshot-metadata, trying to recreate snapshot-metadata");

                try {
                    LibvirtVMSnapshotDef vmSnapshotDef = new LibvirtVMSnapshotDef(vm, command.getTarget(), command.getTarget().getType(), false);

                    for (LibvirtVMDef.DiskDef disk : libvirtComputingResource.getDisks(conn, vm.getName()))
                        try {
                            vmSnapshotDef.addDisk(disk);
                        } catch (LibvirtVMSnapshotDef.UnsupportedDiskException e2) {
                            s_logger.error(e.toString());
                            return new DeleteVMSnapshotAnswer(command, false, e2.toString());
                        }

                    vm.snapshotCreateXML(vmSnapshotDef.toString(), vmSnapshotDef.getFlags());
                } catch(LibvirtException e2){
                    s_logger.error("Unable to recreate VM snapshot-definition: \n" + e2.toString());
                    return new DeleteVMSnapshotAnswer(command, false, "Unable to recreate VM snapshot-definition");
                }
            }

            if(snapshot != null)
                snapshot.delete(0); // Delete snapshot safely by merging it with its children
            else
                return new DeleteVMSnapshotAnswer(command, false, (new CloudRuntimeException("Can't delete the requested snapshot as it does not exist!")).toString());

        } catch (final LibvirtException e) {
            s_logger.error("Something went wrong while trying to make the snapshot with xml: " + xmlSnapshot.toString());
            return new DeleteVMSnapshotAnswer(command, false, e.toString());
        } catch (final CloudRuntimeException e) {
            return new DeleteVMSnapshotAnswer(command, false, e.toString());
        }

        return new DeleteVMSnapshotAnswer(command, command.getVolumeTOs());
    }
}
