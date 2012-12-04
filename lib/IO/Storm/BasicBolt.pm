package IO::Storm::BasicBolt;
use Moose;

extends 'IO::Storm';

use Log::Log4perl;

my $logger = Log::Log4perl->get_logger('storm.basicbolt');

sub initialize {
    my ( $self, $conf, $context ) = @_;
}

sub process {
    my ( $self, $tuple ) = @_;
}

sub run {
    my ($self) = @_;
    $self->mode('bolt');

    # XXX
    $logger->debug('BasicBolt->run');

    my ( $conf, $context ) = $self->init_component;
    $self->initialize( $conf, $context );

    while (1) {
        my $tup = $self->read_tuple;
        $self->_anchor($tup);
        $self->process($tup);
        $self->ack($tup);
    }
    $logger->debug('run: end');
}

1;
