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
public class LibvirtRevertToVMSnapshotCommand extends CommandWrapper<RevertToVMSnapshotCommand, Answer, LibvirtComputingResource> {

    private static final Logger s_logger = Logger.getLogger(LibvirtCreateVMSnapshotCommandWrapper.class);

    @Override
    public Answer execute(RevertToVMSnapshotCommand command, LibvirtComputingResource libvirtComputingResource) {
        final String vmName = command.getVmName();
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
                    s_logger.trace("Ignoring libvirt error.", e);
                }
            }

            DomainSnapshot snapshot = vm.snapshotLookupByName(command.getTarget().getSnapshotName());

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
