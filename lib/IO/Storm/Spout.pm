package IO::Storm::Spout;
use Moose;

extends 'IO::Storm';

use Log::Log4perl;
my $logger = Log::Log4perl->get_logger('storm.spout');

sub initialize {
    my ( $self, $conf, $context ) = @_;
}

sub ack {
    my ( $self, $id ) = @_;
}

sub fail {
    my ( $self, $id ) = @_;
}

sub nextTuple {
    my ($self) = @_;
}

sub run {
    $self->mode = 'spout';

    my ( $conf, $context ) = $self->init_component;
    $self->initialize( $conf, $context );

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
        $self->sync;
    }
}

1;
