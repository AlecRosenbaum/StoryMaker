$(document).ready(function() {
    //  global state variables
    var used,
        available;

    
    // make hovering pretty\
    function hoverEnter(){
        var cls = ["bg-blue", "bg-green", "bg-lightblue", "bg-orange"];
        $(this).addClass(cls[~~(Math.random() * cls.length)]);
    }
    function hoverLeave() {
        var cls = ["bg-blue", "bg-green", "bg-lightblue", "bg-orange"];
        for (var i = cls.length - 1; i >= 0; i--) {
            $(this).removeClass(cls[i]);
        }
    }
    $(".post").hover(hoverEnter, hoverLeave);

    // cycle through sentences
    $("#shuffle").click(function () {
        var $next = $("#next"),
            idx = Math.floor(Math.random() * available.length);
            $next.attr("idx", idx);
            $next.attr("href", available[idx].link);
            $next.attr("sentence_id", available[idx].id);
            $next.text(available[idx].text);
    });

    // Socket io
    namespace = '';
    // Connect to the Socket.IO server.
    var socket = io.connect(location.protocol + '//' + document.domain + ':' + location.port + namespace);

    socket.on('connect', function() {
        socket.emit('join', {
            room: $('#topic').attr("subject_id"),
            subject_id: $('#topic').attr("subject_id")});
    });

    // Event handler for server sent data.
    socket.on('update', function(msg) {
        redraw(msg.data);
    });
    

    $("#submit").click(function () {
        // do nothing for now
        socket.emit('submit', {
            room: $('#topic').attr("subject_id"),
            subject_id: $('#topic').attr("subject_id"),
            sentence_id: $("#next").attr("sentence_id")
        });
    });

    function redraw(data) {
        used = data.used;
        available = data.available;

        var $post_ctr = $("#post_ctr"),
            arrays = [],
            size = 5;

        // clear out old info
        $post_ctr.empty();

        // add next sentece placeholder
        used.push({
            id: "next",
            sentence_id: available[0].id,
            text: available[0].text,
            link: available[0].link
        });

        // split array into groups
        while (used.length > 0) {
            arrays.push(used.splice(0, size));
        }

        // create a paragraph for each group
        arrays.forEach(function(group) {
            var $p = $("<p>");
            group.forEach(function(elem) {
                var $a = $("<a>", {
                        id: elem.id,
                        href: elem.link,
                        "class": "deco-none post"})
                        .text(elem.text + " ");
                $a.hover(hoverEnter, hoverLeave);
                if (elem.id == "next") {
                    $a.attr("sentence_id", elem.sentence_id); 
                }
                
                $p.append($a);              
            });
            $post_ctr.append($p);
        });

        // reset then fix the container height (prevents button jumping)
        $post_ctr.height(null);
        $post_ctr.height($post_ctr.height());

    }
});