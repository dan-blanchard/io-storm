package IO::Storm;

# Imports
use strict;
use warnings;
use v5.10;
use IO::Handle qw(autoflush);
use IO::File;
use Log::Log4perl qw(:easy);
use JSON::XS qw(decode_json encode_json);

use IO::Storm::Tuple;

# Setup Moo for object-oriented niceties
use Moo;
use namespace::clean;

has 'mode' => (
    is      => 'rw',
    default => 'bolt',
);

has '_pending_commands' => (
    is      => 'rw',
    default => sub { [] },
);

has '_pending_taskids' => (
    is      => 'rw',
    default => sub { [] },
);

has '_stdin' => (
    is      => 'rw',
    default => sub {
        my $io = IO::Handle->new;
        $io->fdopen( fileno(STDIN), 'r' );
    }
);

my $MAX_BLANK_MSGS = 500;
my $MAX_LINES      = 100;

my $logger = Log::Log4perl->get_logger('storm');

=method read_message

Read a message from the ShellBolt.  Reads until it finds a "end" line.

=cut

sub read_message {
    $logger->debug('start read_message');
    my $self         = shift;
    my $lines        = 0;
    my $blank_lines  = 0;
    my $message_size = 0;

    my @messages = ();
    while (1) {
        my $line = $self->_stdin->getline;
        $logger->debug( 'read_message: line=' . $line );
        chomp($line);
        if ( $line eq 'end' ) {
            last;
        }
        $lines++;

        # If Storm starts to send us a series of blank lines, after awhile
        # we have to assume that the pipe to the Storm supervisor is broken
        if ( $line eq '' ) {
            $blank_lines++;
            if ( $blank_lines >= $MAX_BLANK_MSGS ) {
                die( "$blank_lines blank lines received, assuming pipe to "
                        . "Storm supervisor is broken" );
            }
        }
        elsif ( $lines >= $MAX_LINES ) {
            die
                "Message exceeds $MAX_LINES lines, assuming this is an error.";
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
        my $msg = $self->read_message();
        while ( ref($msg) ne 'ARRAY' ) {
            push( $self->_pending_commands, $msg );
            $msg = $self->read_message();
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
        my $msg = $self->read_message();
        while ( ref($msg) eq 'ARRAY' ) {
            push( $self->_pending_taskids, $msg );
            $msg = $self->read_message();
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

    my $tupmap = $self->read_command();

    return IO::Storm::Tuple->new(
        id        => $tupmap->{id},
        component => $tupmap->{comp},
        stream    => $tupmap->{stream},
        task      => $tupmap->{task},
        values    => $tupmap->{tuple}
    );
}

=method read_handshake

Read and process an initial handshake message from Storm

=cut

sub read_handshake {
    my $self = shift;

    # TODO: Figure out how to redirect stdout to ensure that print
    # statements/functions won't crash the Storm Java worker

    autoflush STDOUT 1;

    my $msg = $self->read_message();
    $logger->debug( 'Received initial handshake from Storm: %r', $msg );

    # Write a blank PID file out to the pidDir
    my $pid      = $$;
    my $pid_dir  = $msg->{pidDir};
    my $filename = $pid_dir . '/' . $pid;
    open my $fh, '>', $filename
        or die "Cant't write to '$filename': $!\n";
    $fh->close;
    $self->send_message( { pid => $pid } );
    $logger->debug('Process ID sent to Storm');

    return [ $msg->{conf}, $msg->{context} ];
}

=method send_message

Send a message to the ShellBolt, encoding it as JSON.

=cut

sub send_message {
    my ( $self, $msg ) = @_;
    say encode_json($msg);
    say "end";
}

=method sync

Send a sync command to Storm.

=cut

sub sync {
    my $self = shift;

    $self->send_message( { command => 'sync' } );
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
    $self->send_message( \%message );
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
    $self->send_message( \%message );
}

=method log

Send a log command to the ShellBolt

=cut

sub log {
    my $self    = shift;
    my $message = shift;

    $self->send_message( { command => 'log', msg => $message } );
}

1;
