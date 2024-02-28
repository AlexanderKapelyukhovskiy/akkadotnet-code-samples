using Akka.Actor;
using Akka.Event;
using Akka.Persistence;

namespace SqlSharding.Host.Actors;

public record TimerStartedEvent(DateTime ExecutionTime);
public record StartTimerCommand(DateTime ExecutionTime);
public record FireTimerCommand;

public record TimerState
{
    public DateTime ExecutionTime { get; set; }
}

public sealed class PersistentTimer : ReceivePersistentActor, IWithTimers
{
    public static Props GetProps(string timerId)
    {
        return Props.Create(() => new PersistentTimer(timerId));
    }

    public const string PersistentTimerNameConstant = "timer";
    private readonly ILoggingAdapter _log = Context.GetLogger();
    public TimerState State;

    public PersistentTimer(string timerId)
    {
        PersistenceId = $"{PersistentTimerNameConstant}-{timerId}";
        State = new TimerState();

        Recover<TimerStartedEvent>(timerEvent =>
        {
            _log.Warning("Recover<TimerStartedEvent> [{0}]", timerEvent.ExecutionTime);
            State = new TimerState { ExecutionTime = timerEvent.ExecutionTime };
        });

        Recover<SnapshotOffer>(offer =>
        {
            if (offer.Snapshot is TimerState state)
            {
                _log.Warning("Recover<SnapshotOffer> [{0}]", state.ExecutionTime);
                State = state;
            }
        });

        Recover<RecoveryCompleted>(_ =>
        {
            _log.Warning("Recover<RecoveryCompleted>");
            if (State.ExecutionTime != DateTime.MinValue)
            {
                _log.Warning("Recover<RecoveryCompleted> [{0}]", State.ExecutionTime);
                StartTimer();
            }
        });

        Command<StartTimerCommand>(timerCommand =>
        {
            var timerEvent = new TimerStartedEvent(timerCommand.ExecutionTime);
            State = new TimerState {ExecutionTime = timerEvent.ExecutionTime};
            Persist(timerEvent, _ => { StartTimer(); });
        });

        Command<FireTimerCommand>(_ =>
        {
            _log.Warning("FireTimerCommand [{0}]", DateTime.UtcNow - State.ExecutionTime );
        });
    }

    private void StartTimer()
    {
        TimeSpan timeout = State.ExecutionTime - DateTime.UtcNow;
        Timers.StartSingleTimer(PersistenceId, new FireTimerCommand(), timeout);
        _log.Warning("StartTimer [{0}]", timeout);
    }

    public override string PersistenceId { get; }

    public ITimerScheduler Timers { get; set; }
}