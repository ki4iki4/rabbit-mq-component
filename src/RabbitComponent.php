<?php
/**
 * Created by PhpStorm.
 * User: bigdrop
 * Date: 6/19/18
 * Time: 3:15 PM
 */

namespace ki4iki4\rabbitmq\src;


use AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;
use yii\base\Component;

class RabbitComponent extends Component
{
    /**
     * @var AMQPStreamConnection
     */
    private $connection;

    /** @var AMQPChannel $channel */
    private $channel;
    private $callback_queue;
    private $response;
    private $corr_id;
    public $host;
    public $port;
    public $user;
    public $pass;
    public $queue;
    public $args;
    public $vhost;

    public function onResponse($rep)
    {
        if ($rep->get('correlation_id') == $this->corr_id) {
            $this->response = $rep->body;
        }
    }

    /**
     * @param $msg
     * @return string
     */
    public function rpcCall($msg)
    {
        $this->channel = $this->connection->channel();
        list($this->callback_queue, ,) = $this->channel->queue_declare(
            "",
            false,
            false,
            true,
            false,
            false,
            $this->args = []
        );
        $this->channel->basic_consume(
            $this->callback_queue,
            '',
            false,
            false,
            false,
            false,
            [
                $this,
                'onResponse',
            ]
        );
        $this->response = null;
        $this->corr_id = uniqid();

        $msg = new AMQPMessage(
            $msg,
            [
                'correlation_id' => $this->corr_id,
                'reply_to'       => $this->callback_queue,
            ]
        );
        $this->channel->basic_publish($msg, '', 'rpc_queue');
        while (!$this->response) {
            $this->channel->wait();
        }
        $this->channel->close();

        return $this->response;
    }

    /**
     * @param $msg
     */
    public function sendMsg($msg)
    {
        $this->channel = $this->connection->channel();
        $this->channel->queue_declare($this->queue, false, true, false, false,false,$this->args = []);
        $msg = new AMQPMessage($msg);
        $this->channel->basic_publish($msg, '', $this->queue);
        $this->channel->close();
    }

    /**
     * @return AMQPStreamConnection
     */
    public function openConnection()
    {
        if (!isset($this->connection)) {
            $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->pass, $this->vhost);
        }

        return $this->connection;
    }

    public function closeConnection()
    {
        if (isset($this->connection)) {
            $this->connection->close();
        }
    }
}
