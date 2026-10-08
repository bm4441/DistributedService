package com.zebra.ai.distributed;

/**
 * What the service is at the moment: a Contributor, which lends this device to the requestors that ask for it, or a
 * Requestor, which asks other devices for their help. It is a Contributor by default, and a Requestor while an
 * application -- the demo application, say -- is bound to it through its AIDL interface (see
 * DistributedService.getServiceMode()). What each mode does is still to come: for now the mode is only logged.
 */
enum ServiceMode {

    CONTRIBUTOR("Contributor"),
    REQUESTOR("Requestor");

    /** How the mode is written in the log: "The Service Mode: Contributor". */
    final String label;

    ServiceMode(String label) {
        this.label = label;
    }
}
