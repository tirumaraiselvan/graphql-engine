Async Actions
=============


.. contents:: Table of contents
  :backlinks: none
  :depth: 1
  :local:


Sometimes, your business logic is lengthy or asynchronous. You can take advantage of subscriptions over actions in this case by providing the ``action_id``.

.. code-block:: graphql

   subscription order_status($action_id: uuid!) {
     place_order(action_id: $action_id) {
       order {
         id
         payment_url
         total_amount
         discount
       }
     }
   }


