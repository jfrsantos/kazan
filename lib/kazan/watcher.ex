defmodule Kazan.Watcher do
  @moduledoc """

  Watches for changes on a resource.  This works by creating a HTTPoison ASync request.
  The request will eventually timeout, however the Watcher handles recreating the request
  when this occurs and requesting new events that have occurred since the last *resource_version*
  received.

  To use:

  1. Create a request in the normal way, but setting `watch=true` e.g.
  ```
  request = Kazan.Apis.CoreV1.list_namespace!(watch: "true")
  ```
  2. Start a watcher using the request, and passing the initial resource version
  and a pid to which to send received messages.  The initial *resource_version* can be
  obtained by doing a normal, non-watch request to the same endpoint.
  ```
  Kazan.Watcher.start_link(request, resource_version: rv, send_to: self())
  ```
  3. In your client code you receive messages for each `%WatchEvent{}`.
    You can pattern match on the object type if you have multiple watchers configured.
    For example, if the client is a `GenServer`
  ```
    alias Kazan.Models.Apimachinery.Meta.V1.WatchEvent

    # type is "ADDED", "DELETED" or "MODIFIED"
    def handle_info(%WatchEvent{object: object, type: type}, state) do
      case object do
        %Kazan.Models.Core.V1.Namespace{} = namespace ->
          process_namespace_event(type, namespace)

        %Kazan.Models.Core.V1.Job{} = job ->
          process_job_event(type, job)
      end
      {:noreply, state}
    end
  ```
  """

  use GenServer
  alias Kazan.LineBuffer
  alias Kazan.Models.Apimachinery.Meta.V1.WatchEvent
  require Logger

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

    defstruct id: nil, request: nil, send_to: nil, server: nil, buffer: LineBuffer.new(), rv: nil
  end

  @doc """
  Starts a watch request to the kube server

  The server should be set in the kazan config or provided in the options.

  ### Options

  * `server` - A `Kazan.Server` struct that defines which server we should send
    this request to. This will override any server provided in the Application
    config.
  * `send_to` - A `pid` to which events are sent.  Defaults to `self()`.
  * `resource_version` - The version from which to start watching.
  """
  def start_link(%Kazan.Request{} = request, opts) do
    send_to = Keyword.get(opts, :send_to, self())
    rv = Keyword.fetch!(opts, :resource_version)
    server = Keyword.get(opts, :server)
    GenServer.start_link(__MODULE__, [request, rv, send_to, server])
  end

  @impl GenServer
  def init([request, rv, send_to, server]) do
    Logger.info "Watcher init pid: #{inspect self()} rv: #{rv} request: #{inspect request}"
    {:ok, id} = Kazan.Client.run(request, stream_to: self(), server: server)
    {:ok, %State{id: id, request: request, rv: rv, send_to: send_to, server: server}}
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
  def handle_info(%HTTPoison.AsyncChunk{chunk: chunk, id: request_id},
                  %State{id: id, buffer: buffer, rv: rv, send_to: send_to} = state) do
    if request_id != id do
      Logger.warn "Received chunk for wrong request_id: #{inspect request_id} expect: #{inspect id}"
      {:noreply, state}
    else

      {lines, buffer} =
        buffer
        |> LineBuffer.add_chunk(chunk)
        |> LineBuffer.get_lines()

        new_rv = process_lines(lines, rv, send_to)

      {:noreply, %State{state | buffer: buffer, rv: new_rv}}
    end
  end

  @impl GenServer
  def handle_info(%HTTPoison.Error{reason: {:closed, :timeout}}, state) do
    Logger.info "Received Timeout with id: #{inspect self()}"
    {:noreply, restart_connection(state)}
  end

  @impl GenServer
  def handle_info(%HTTPoison.AsyncEnd{id: request_id},
                  %State{id: id} = state) do
    Logger.info "Received AsyncEnd for id: #{inspect request_id}"
    if request_id != id do
      Logger.warn "Received AsyncEnd message for wrong request_id: #{inspect request_id} expect: #{inspect id}"
      {:noreply, state}
    else
      {:noreply, restart_connection(state)}
    end
  end


  # INTERNAL

  defp restart_connection(%State{request: request, rv: rv, server: server} = state) do
    query_params = Map.put(request.query_params, "resourceVersion", rv)
    request = %Kazan.Request{request | query_params: query_params}
    {:ok, id} = Kazan.Client.run(request, stream_to: self(), server: server)
    Logger.info "Restarted #{inspect self()}"
    %State{state | id: id, buffer: LineBuffer.new()}
  end

  defp process_lines(lines, rv, send_to) do
    Enum.reduce(lines, rv, fn(line, current_rv) ->
      {:ok, %{"type" => type, "object" => raw_object}} = Poison.decode(line)
      {:ok, model} = Kazan.Models.decode(raw_object)
      ve = %WatchEvent{type: type, object: model}

      send(send_to, ve)

      new_rv(type, raw_object, current_rv)
    end)
  end

  defp new_rv("DELETED", _object, current_rv), do: current_rv
  defp new_rv(type, object, current_rv) do
    case object do
      %{"metadata" => %{"resourceVersion" => new_rv}} ->
        if String.to_integer(new_rv) > String.to_integer(current_rv) do
          new_rv
        else
          Logger.warn "#{inspect self()} Old rv found: #{new_rv} type: #{type} current: #{current_rv} kind: #{object["kind"]} ns: #{object["metadata"]["namespace"]} name: #{object["metadata"]["name"]}"
          current_rv
        end
      _ ->
        Logger.warn "No rv found in object"
        current_rv
    end
  end
end
