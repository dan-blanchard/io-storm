package IO::Storm;
use Moose;

use Data::Dumper;
use IO::Handle qw(autoflush);
use IO::File;
use JSON::XS qw(decode_json encode_json);
use Log::Log4perl;

my $logger = Log::Log4perl->get_logger('storm');
use IO::Storm::Tuple;

has 'mode' => (
    is      => 'rw',
    isa     => 'Str',
    default => 'bolt',
);

has '_anchor' => (
    is        => 'rw',
    isa       => 'IO::Storm::Tuple',
    predicate => '_has_anchor'
);

has '_pending_commands' => (
    is      => 'rw',
    isa     => 'ArrayRef',
    default => sub { [] },
);

has '_pending_taskids' => (
    is      => 'rw',
    isa     => 'ArrayRef',
    default => sub { [] },
);

has '_stdin' => (
    is      => 'rw',
    default => sub {
        my $io = IO::Handle->new;
        $io->fdopen( fileno(STDIN), 'r' );
    }
);

=method read_message

Read a message from the ShellBolt.  Reads until it finds a "end" line.

=cut

sub read_message {
    $logger->debug('start read_message');
    my $self = shift;

    my @messages = ();
    while (1) {
        my $line = $self->_stdin->getline;
        $logger->debug( 'read_message: line=' . $line );
        chomp($line);
        if ( $line eq 'end' ) {
            last;
        }
        push( @messages, $line );
    }

    return decode_json( join( "\n", @messages ) );
}

sub read_task_ids {
    my $self = shift;

    if ( @{ $self->_pending_taskids } ) {
        return shift( $self->_pending_taskids );
    }
    else {
        my $msg = $self->read_message;
        while ( ref($msg) ne 'ARRAY' ) {
            push( $self->_pending_commands, $msg );
            $msg = $self->read_message;
        }

        return $msg;
    }
}

sub read_command {
    my $self = shift;

    if ( @{ $self->_pending_commands } ) {
        return shift( $self->_pending_commands );
    }
    else {
        my $msg = $self->read_message;
        while ( ref($msg) eq 'ARRAY' ) {
            push( $self->_pending_taskids, $msg );
            $msg = $self->read_message;
        }
        return $msg;
    }
}

=method read_tuple

Turn the incoming Tuple structure into an <IO::Storm::Tuple>.

=cut

sub read_tuple {
    my $self = shift;
    $logger->debug('read_tuple');

    my $tupmap = $self->read_command;

    return IO::Storm::Tuple->new(
        id        => $tupmap->{id},
        component => $tupmap->{comp},
        stream    => $tupmap->{stream},
        task      => $tupmap->{task},
        values    => $tupmap->{tuple}
    );
}

=method send_message_to_parent

Sent a message to the ShellBolt, encoding it as JSON.

=cut

sub send_message_to_parent {
    my $self = shift;
    my $msg  = shift;

    $self->_send_to_parent( encode_json($msg) );
}

sub sync {
    my $self = shift;

    $self->send_message_to_parent( { command => 'sync' } );
}

=method send_pid

Send this processes PID.

=cut

sub send_pid {
    my $self  = shift;
    my $hbdir = shift;

    my $pid = $$;

    $self->send_message_to_parent( { pid => $pid } );
    $logger->debug("send_pid: sent $pid");

    # XXX error handling
    my $filename = $hbdir . '/' . $pid;
    open my $fh, '>', $filename
      or die "Cant't write to '$filename': $!\n";
    $fh->close;
}

=method emit_direct

Emit a tuple to the Shell bolt, but do not get a response.

=cut

sub emit_direct {
    my $self = shift;
    my $args = shift;

    if ( $self->mode eq 'bolt' ) {
        $self->emit_bolt($args);
    }
    elsif ( $self->mode eq 'spout' ) {
        $self->emit_spout($args);
    }
}

=method emit_bolt

Emit a tuple to the Shell bolt, and return the response.

=cut

sub emit_bolt {
    my $self    = shift;
    my $tuple   = shift;
    my $args    = shift;
    my %message = ( command => 'emit' );

    if ( defined( $args->{stream} ) ) {
        $message{stream} = $args->{stream};
    }

    if ( $self->_has_anchor ) {

        # The python implementation maps this, but just works with a single
        # anchor.  Perhaps it was there for some other feature?
        $message{anchors} = [ $self->_anchor->id ];
    }

    if ( defined( $args->{direct_task} ) ) {
        $message{task} = $args->{direct_task};
    }

    $message{tuple} = $tuple;
    $logger->debug("emit_bolt: tuple $tuple");
    $self->send_message_to_parent( \%message );
}

=method emit_spout

Emit a tuple to the Shell spout

=cut

sub emit_spout {
    my $self    = shift;
    my $tuple   = shift;
    my $args    = shift;
    my %message = ( command => 'emit' );

    if ( defined( $args->{id} ) ) {
        $message{id} = $args->{id};
    }

    if ( defined( $args->{stream} ) ) {
        $message{stream} = $args->{stream};
    }

    if ( defined( $args->{direct_task} ) ) {
        $message{task} = $args->{direct_task};
    }

    $message{tuple} = $tuple;
    $logger->debug("emit_spout: tuple $tuple");
    $self->send_message_to_parent( \%message );
}

=method emit

Emit a tuple to the ShellBolt or ShellSpout and return the response.

=cut

sub emit {
    my $self = shift;
    my $args = shift;

    if ( $self->mode eq 'bolt' ) {
        $self->emit_bolt($args);
    }
    elsif ( $self->mode eq 'spout' ) {
        $self->emit_spout($args);
    }

    return $self->read_task_ids;
}

=method ack

Acknowledge a tuple.

=cut

sub ack {
    my $self  = shift;
    my $tuple = shift;

    $self->send_message_to_parent( { command => 'ack', id => $tuple->id } );
}

=method fail

Fail a tuple.

=cut

sub fail {
    my $self  = shift;
    my $tuple = shift;

    $self->send_message_to_parent( { command => 'fail', id => $tuple->id } );
}

=method log

Send a log command to the ShellBolt

=cut

sub log {
    my $self    = shift;
    my $message = shift;

    $self->send_message_to_parent( { command => 'log', msg => $message } );
}

=method init_bolt

Initialize this bolt.

=cut

sub init_component {
    my $self = shift;

    autoflush STDOUT 1;

    my $setup_info = $self->read_message;

    $self->send_pid( $setup_info->{pidDir} );

    return [ $setup_info->{conf}, $setup_info->{context} ];
}

=method _send_to_parent

private method that sends message to ShellBolt

=cut

sub _send_to_parent {
    my ( $self, $s ) = @_;

    $logger->debug("sending $s");
    print "$s\n";
    $logger->debug('sending end');
    print "end\n";
}

1;
