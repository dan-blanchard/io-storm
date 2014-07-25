# ABSTRACT: Storm's primitive data type passed around via streams.

package IO::Storm::Tuple;

# Setup Moo for object-oriented niceties
use Moo;
use namespace::clean;

has 'id' => ( is => 'rw' );

has 'component' => ( is => 'rw' );

has 'stream' => ( is => 'rw' );

has 'task' => ( is => 'rw' );

has 'values' => ( is => 'rw' );

sub TO_JSON {
    my ($self) = @_;
    return {
        id        => $self->id,
        component => $self->component,
        stream    => $self->stream,
        task      => $self->task,
        values    => $self->values
    };
}

1;
