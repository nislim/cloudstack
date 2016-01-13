package com.cloud.hypervisor.kvm.resource.wrapper;

import com.cloud.agent.api.Answer;
import com.cloud.agent.api.DeleteVMSnapshotAnswer;
import com.cloud.agent.api.DeleteVMSnapshotCommand;
import com.cloud.hypervisor.kvm.resource.LibvirtComputingResource;
import com.cloud.resource.CommandWrapper;
import com.cloud.resource.ResourceWrapper;
import com.cloud.utils.exception.CloudRuntimeException;
import org.apache.log4j.Logger;
import org.libvirt.*;

/**
 * Created by nick on 12/01/2016.
 */

@ResourceWrapper(handles = DeleteVMSnapshotCommand.class)
public class LibvirtDeleteVMSnapshotCommandWrapper  extends CommandWrapper<DeleteVMSnapshotCommand, Answer, LibvirtComputingResource> {

    private static final Logger s_logger = Logger.getLogger(LibvirtDeleteVMSnapshotCommandWrapper.class);

    @Override
    public Answer execute(DeleteVMSnapshotCommand command, LibvirtComputingResource libvirtComputingResource) {
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

            DomainSnapshot snapshot = vm.snapshotLookupByName(command.getTarget().getSnapshotName());

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
