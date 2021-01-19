defmodule Kazan.Watcher do
  @moduledoc """

  Watches for changes on a resource.  This works by creating a HTTPoison ASync request.
  The request will eventually timeout, however the Watcher handles recreating the request
  when this occurs and requesting new events that have occurred since the last *resource_version*
  received.

  # Usage

  1. Create a request in the normal way that supports the `watch` parameter.  Alternatively
  create a watch request.
  ```
  request = Kazan.Apis.Core.V1.list_namespace!() # No need to add watch: true
  ```
  or
  ```
  request = Kazan.Apis.Core.V1.watch_namespace_list!()
  ```
  2. Start a watcher using the request, and passing the initial resource version
  and a pid to which to send received messages.
  ```
  Kazan.Watcher.start_link(request, resource_version: rv, send_to: self())
  ```
  If no `resource_version` is passed then the watcher initially makes a normal
  request to get the starting value.  This will only work if a non `watch` request
  is used. i.e. `Kazan.Apis.Core.V1.list_namespace!()`
  rather than `Kazan.Apis.Core.V1.watch_namespace_list!()`

  3. In your client code you receive messages for each `%Watcher.Event{}`.
    You can pattern match on the object type if you have multiple watchers configured.
    For example, if the client is a `GenServer`
  ```
    # type is `:added`, `:deleted`, `:modified` or `:gone`
    def handle_info(%Watcher.Event{object: object, from: watcher_pid, type: type}, state) do
      case object do
        %Kazan.Apis.Core.V1.Namespace{} = namespace ->
          process_namespace_event(type, namespace)

        %Kazan.Apis.Batch.V1.Job{} = job ->
          process_job_event(type, job)
      end
      {:noreply, state}
    end
  ```

  4. In the case that a `:gone` is received, then this indicates that Kubernetes
     has sent a 410 error.  In this case the Watcher will automatically terminate
     and the consumer must clear its cache, reload any cached resources and
     restart the watcher.


  A Watcher can be terminated by calling `stop_watch/1`.

  # Having the Watcher managing `:gone` events

    If you pass `manage_gone?: true` as an option to `start_link/2` then the Watcher
    will manage `:gone` events for you.  In this case do not pass `resource_version`.

    The Watcher will then fetch the initial version of the resource and send it to the consumer
    via the following message:

    ```
    %Watcher.Event{object: object, from: watcher_pid, type: :init}
    ```

    When a `:gone` event is received by the Watcher, it will refetch the resource and start watching it again.  The resource
    is sent to the consumer:

    ```
    %Watcher.Event{object: object, from: watcher_pid, type: :sync}

    ```

    In this case the consumer must also reload its cache based on the object passed,
    but it does not need to restart the watch.

  """

  use GenServer
  alias Kazan.LineBuffer
  require Logger

  defmodule InvalidOptions do
    @moduledoc "Raised when options to start the watch are invalid"
    defexception [:reason]

    def message(error) do
      "Invalid options for watcher: #{error.reason}"
    end
  end

  defmodule State do
    @moduledoc false

    # Stores the internal state of the watcher.
    #
    # Includes:
    #
    # resource_version:
    #
    # The K8S *resource_version* (RV) is used to version each change in the cluster.
    # When we listen for events we need to specify the RV to start listening from.
    # For each event received we also receive a new RV, which we store in state.
    # When the watch eventually times-out (it is an HTTP request with a chunked response),
    # we can then create a new request passing the latest RV that was received.
    #
    # buffer:
    #
    # Chunked HTTP responses are not always a complete line, so we must buffer them
    # until we have a complete line before parsing.

    defstruct id: nil,
              request: nil,
              send_to: nil,
              name: nil,
              manage_gone?: nil,
              ref: nil,
              buffer: nil,
              rv: nil,
              max_connect_retries: nil,
              client_opts: [],
              log_level: false
  end

  defmodule Event do
    defstruct [:type, :object, :from, :ref]
  end

  @doc """
  Starts a watch request to the kube server

  The server should be set in the kazan config or provided in the options.

  ### Options

  * `send_to` - A `pid` to which events are sent.  Defaults to `self()`.
  * `resource_version` - The version from which to start watching. Raises if set and `manage_gone?` is `true`.
  * `name` - An optional name for the watcher.  Displayed in logs.
  * `registered_name` - An optional name (atom) or via tuple to register the process with.
  * `ref` - An optional ref that is sent in the events.  If you are watching multiple resources you can use
    this to identify from which watcher they are coming. Although the watcher pid is also sent in the event this
    can be more convenient. A typical use would be a cluster name in the case of having multiple K8S clusters. Can be any
    valid term.
  * `manage_gone?` - Have the Watcher manage `:gone` events.  See above.  Defaults to `false.
  * `log` - the level to log. When false, disables watcher logging.
  * 'max_connect_retries' - if a K8S connection error occurs, by default the watcher will raise an exception.
    Setting this to a value > 0 will retry the specified number of times with an exponetial backoff starting at 1 second,
    and a maximum of 2 minutes, before raising. A value of `:infinity` will retry forever.
  * Other options are passed directly to `Kazan.Client.run/2`
  """
  def start_link(%Kazan.Request{} = request, opts) do
    {send_to, watcher_opts} = Keyword.pop(opts, :send_to, self())
    # Registered name is the name or via tuple to register the watcher process
    # We cannot use `name` as this is already used as the name of the watcher itself.
    {registered_name, watcher_opts} =
      Keyword.pop(watcher_opts, :registered_name)

    genserver_opts =
      case registered_name do
        nil -> []
        name -> [name: name]
      end

    GenServer.start_link(
      __MODULE__,
      [request, send_to, watcher_opts],
      genserver_opts
    )
  end

  @doc "Stops the watch and terminates the process"
  def stop_watch(pid) do
    # Need to catch the case where the watch might have already terminated due
    # to a GONE already having being received.  This can happen if the send_to process
    # has multiple watches and it manually tries to stop other watches once a GONE is received
    # on two at the same time.
    try do
      GenServer.call(pid, :stop_watch)
    catch
      :exit, {:noproc, _} -> :already_stopped
    end
  end

  @impl GenServer
  def init([request, send_to, opts]) do
    {manage_gone?, opts} = Keyword.pop(opts, :manage_gone?, false)
    {ref, opts} = Keyword.pop(opts, :ref)
    {rv, opts} = Keyword.pop(opts, :resource_version)
    {name, opts} = Keyword.pop(opts, :name, inspect(self()))
    {log_level, opts} = Keyword.pop(opts, :log, false)
    {max_connect_retries, opts} = Keyword.pop(opts, :max_connect_retries, 0)

    if manage_gone? && rv do
      raise InvalidOptions,
        reason: "resource_version cannot be set when manage_gone? is true"
    end

    rv =
      case rv do
        nil ->
          log(log_level, "Obtaining initial rv")

          {:ok, object} = Kazan.run(request, Keyword.put(opts, :timeout, 500))

          if manage_gone? do
            send(send_to, %Event{
              type: :init,
              from: self(),
              object: object,
              ref: ref
            })
          end

          extract_rv(object)

        rv ->
          rv
      end

    client_opts =
      opts
      |> Keyword.put_new(:recv_timeout, 5 * 60 * 1000)

    log(
      log_level,
      "Watcher init: #{name} rv: #{rv} request: #{inspect(request)}"
    )

    state = %State{
      request: request,
      rv: rv,
      send_to: send_to,
      manage_gone?: manage_gone?,
      name: name,
      ref: ref,
      client_opts: client_opts,
      log_level: log_level,
      max_connect_retries: max_connect_retries
    }

    # Monitor send_to process so we can terminate when it goes down
    Process.monitor(send_to)
    send(self(), :start_request)
    {:ok, state}
  end

  @impl GenServer
  def handle_call(:stop_watch, _from, %State{} = state) do
    log(state, "Stopping watch #{inspect(self())}")
    {:stop, :normal, :ok, state}
  end

  @impl GenServer
  def handle_info(:start_request, state) do
    {:noreply, start_request(state)}
  end

  @impl GenServer
  # should not be used normally - just used for testing and debugging to force
  # a GONE error by faking a chunk from HTTPoison
  # N.B. This will leave the HTTPoison transformer process still running which
  # with a real K8S cluster should die as the connection is closed
  # TODO: Perhaps try sending this to HTTPoison instead but this also does not simulate the connection closing
  # on the server and may cause extra messages when HTTPoison closes the connection on its side
  def handle_info(:force_gone, %State{id: request_id} = state) do
    Logger.warn("Forcing gone - SHOULD BE USED IN TESTING ONLY")

    chunk = %{
      "type" => "gone",
      "object" => %{
        "apiVersion" => "v1",
        "code" => 410,
        "kind" => "Status",
        "reason" => "Gone",
        "status" => "Failure",
        "message" => "Forced gone for testing"
      }
    }

    send(self(), %HTTPoison.AsyncChunk{
      chunk: Poison.encode!(chunk) <> "\n",
      id: request_id
    })

    # Simulate ASyncEnd also being sent with old request ID a few 100ms after
    Process.send_after(self(), %HTTPoison.AsyncEnd{id: request_id}, 1000)
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(%HTTPoison.AsyncStatus{code: 200}, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(%HTTPoison.AsyncHeaders{}, state) do
    {:noreply, state}
  end

  @impl GenServer
  def handle_info(
        %HTTPoison.AsyncChunk{chunk: chunk, id: request_id},
        %State{
          id: request_id,
          buffer: buffer,
          send_to: send_to,
          ref: ref,
          manage_gone?: manage_gone?
        } = state
      ) do
    {lines, buffer} =
      buffer
      |> LineBuffer.add_chunk(chunk)
      |> LineBuffer.get_lines()

    case process_lines(state, lines) do
      {:ok, new_rv} ->
        {:noreply, %State{state | buffer: buffer, rv: new_rv}}

      {:error, :gone} ->
        if manage_gone? do
          state =
            state
            |> refetch_resource()
            |> start_request()

          {:noreply, state}
        else
          send(send_to, %Event{type: :gone, from: self(), ref: ref})
          {:stop, :normal, state}
        end
    end
  end

  @impl GenServer
  def handle_info(
        %HTTPoison.Error{id: request_id, reason: {:closed, :timeout}},
        %State{id: request_id, name: name, rv: rv} = state
      ) do
    log(state, "Received Timeout: #{name} rv: #{rv}")
    {:noreply, start_request(state)}
  end

  @impl GenServer
  def handle_info(
        %HTTPoison.AsyncEnd{id: request_id},
        %State{id: request_id, name: name, rv: rv} = state
      ) do
    log(state, "Received AsyncEnd: #{name} rv: #{rv}")
    {:noreply, start_request(state)}
  end

  @impl GenServer
  def handle_info(
        %HTTPoison.AsyncEnd{id: request_id},
        %State{name: name} = state
      ) do
    log(
      state,
      "Received AsyncEnd #{name} from old request id: #{inspect(request_id)} - ignoring"
    )

    {:noreply, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, reason}, %State{} = state) do
    %State{name: name} = state

    log(
      state,
      "#{inspect(self())} - #{name} send_to process #{inspect(pid)} :DOWN reason: #{
        inspect(reason)
      }"
    )

    {:stop, :normal, state}
  end

  # INTERNAL

  defp refetch_resource(
         %State{
           request: request,
           ref: ref,
           send_to: send_to,
           client_opts: client_opts
         } = state
       ) do
    {:ok, object} = Kazan.run(request, Keyword.put(client_opts, :timeout, 500))

    send(send_to, %Event{
      type: :sync,
      from: self(),
      object: object,
      ref: ref
    })

    rv = extract_rv(object)

    %State{state | rv: rv}
  end

  defp start_request(
         %State{
           request: request,
           name: name,
           rv: rv,
           max_connect_retries: max_retries,
           client_opts: client_opts
         } = state,
         retries \\ 0
       ) do
    query_params =
      request.query_params
      |> Map.put("watch", true)
      |> Map.put("resourceVersion", rv)

    request = %Kazan.Request{request | query_params: query_params}

    case Kazan.run(request, [{:stream_to, self()} | client_opts]) do
      {:ok, id} ->
        log(state, "Started request: #{name} rv: #{rv} id: #{inspect(id)}")
        %State{state | id: id, buffer: LineBuffer.new()}

      {:error, %HTTPoison.Error{reason: reason}} ->
        if max_retries == :infinity or retries < max_retries do
          # Exponential backoff starting 1 second, max of 2 minutes
          delay_s =
            :math.pow(2, retries)
            |> round()
            |> min(120)

          log(
            state,
            "Cannot start request: #{name} rv: #{rv} retries: #{retries} reason: #{
              reason
            } sleeping #{delay_s}s before retrying"
          )

          :timer.sleep(delay_s * 1000)
          start_request(state, retries + 1)
        else
          raise "Cannot start watch due to connection error #{name} rv: #{rv} retries: #{
                  retries
                } reason: #{inspect(reason)}"
        end
    end
  end

  defp process_lines(%State{name: name, rv: rv} = state, lines) do
    log(state, "Process lines: #{name}")

    Enum.reduce(lines, {:ok, rv}, fn line, status ->
      case status do
        {:ok, current_rv} ->
          process_line(line, current_rv, state)

        # stop on error
        {:error, :gone} ->
          {:error, :gone}
      end
    end)
  end

  # Processes each line returning either {:ok, new_rv} or {:error, :gone}
  # corresponding messages are sent to the send_to process
  defp process_line(line, current_rv, %State{} = state) do
    %State{
      name: name,
      ref: ref,
      send_to: send_to
    } = state

    {:ok, %{"type" => type, "object" => raw_object}} = Poison.decode(line)
    {:ok, model} = Kazan.Models.decode(raw_object, nil)

    case extract_rv(raw_object) do
      {:gone, message} ->
        log(state, "Received 410: #{name} message: #{message}.")
        {:error, :gone}

      ^current_rv ->
        log(state, "Duplicate message: #{name} type: #{type} rv: #{current_rv}")

        {:ok, current_rv}

      new_rv ->
        log(state, "Received message: #{name} type: #{type} rv: #{new_rv}")

        send(send_to, %Event{
          type: type_to_atom(type),
          object: model,
          from: self(),
          ref: ref
        })

        {:ok, new_rv}
    end
  end

  defp type_to_atom("ADDED"), do: :added
  defp type_to_atom("MODIFIED"), do: :modified
  defp type_to_atom("DELETED"), do: :deleted

  defp extract_rv(%{
         "code" => 410,
         "kind" => "Status",
         "reason" => reason,
         "status" => "Failure",
         "message" => message
       })
       when reason in ["Gone", "Expired"],
       do: {:gone, message}

  defp extract_rv(%{"metadata" => %{"resourceVersion" => rv}}), do: rv
  defp extract_rv(%{metadata: %{resource_version: rv}}), do: rv

  defp log(%State{log_level: log_level}, msg), do: log(log_level, msg)
  defp log(false, _), do: :ok

  defp log(log_level, msg) do
    Logger.log(log_level, fn -> msg end)
  end
end
