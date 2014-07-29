# ABSTRACT: The base class for all IO::Storm Spout.

package IO::Storm::Spout;

use Moo;
use namespace::clean;

extends 'IO::Storm';

=method initialize

Called immediately after the initial handshake with Storm and before the main
run loop. A good place to initialize connections to data sources.

=cut

sub initialize {
    my ( $self, $storm_conf, $context ) = @_;
}

=method ack

Called when a bolt acknowledges a tuple in the topology.

=cut

sub ack {
    my ( $self, $id ) = @_;
}

=method fail

Called when a tuple fails in the topology

A Spout can choose to emit the tuple again or ignore the fail. The default is
to ignore.

=cut

sub fail {
    my ( $self, $id ) = @_;
}

=method next_tuple

Implement this function to emit tuples as necessary.

This function should not block, or Storm will think the spout is dead. Instead,
let it return and streamparse will send a noop to storm, which lets it know the
spout is functioning.

=cut

sub next_tuple {
    my ($self) = @_;
}

=method emit

Emit a spout tuple message.

:param tup: the tuple to send to Storm.  Should contain only
            JSON-serializable data.
:type tup: list
:param tup_id: the ID for the tuple. Leave this blank for an
               unreliable emit.
:type tup_id: str
:param stream: ID of the stream this tuple should be emitted to.
               Leave empty to emit to the default stream.
:type stream: str
:param direct_task: the task to send the tuple to if performing a
                    direct emit.
:type direct_task: int
=cut

sub emit ($$;$) {
    my ( $self, $tuple, $args ) = @_;

    my $msg = { command => 'emit', tuple => $tuple };

    if ( defined($args) ) {
        if ( defined( $args->{tup_id} ) ) {
            $msg->{id} = $args->{tup_id};
        }
        if ( defined( $args->{stream} ) ) {
            $msg->{stream} = $args->{stream};
        }
        if ( defined( $args->{direct_task} ) ) {
            $msg->{task} = $args->{direct_task};
        }
    }

    $self->send_message($msg);

    if ( defined $msg->{task} ) {
        return $msg->{task};
    }
    else {
        return $self->read_task_ids();
    }
}

=method run

Main run loop for all spouts.

Performs initial handshake with Storm and reads tuples handing them off to
subclasses.  Any exceptions are caught and logged back to Storm prior to the
Perl process exiting.

Subclasses should **not** override this method.

=cut

sub run {
    my ($self) = @_;

    my ( $storm_conf, $context ) = $self->read_handshake();
    $self->_setup_component( $storm_conf, $context );
    $self->initialize( $storm_conf, $context );

    while (1) {
        my $msg = $self->read_command();
        if ( $msg->{command} eq 'next' ) {
            $self->next_tuple;
        }
        elsif ( $msg->{command} eq 'ack' ) {
            $self->ack( $msg->{id} );
        }
        elsif ( $msg->{command} eq 'fail' ) {
            $self->fail( $msg->{id} );
        }
        $self->sync();
    }
}

1;
